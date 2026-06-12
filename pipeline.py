import os
import hashlib
import pandas as pd
import duckdb
from datetime import datetime

# Diretório para armazenamento dos dados estruturados
DATA_LAKE_DIR = os.path.join(os.path.dirname(__file__), "data_lake")
PARQUET_FILE = os.path.join(DATA_LAKE_DIR, "users_analytical.parquet")
DUCKDB_FILE = os.path.join(DATA_LAKE_DIR, "camplog_warehouse.db")

# Garantir que a pasta do Data Lake exista
os.makedirs(DATA_LAKE_DIR, exist_ok=True)

# Salt do sistema para mascarar dados sensíveis de forma irreversível (Conformidade LGPD)
ANALYTIC_SALT = os.getenv("DATA_PLATFORM_SALT", "camplog_secure_salt_2026_prod")

def mask_name(name: str) -> str:
    """
    Mascara o nome completo mantendo apenas as primeiras letras para permitir 
    análises demográficas leves, sem expor a identidade (ex: 'Matheus Araujo' -> 'M****** A*****').
    """
    if not name:
        return "Anonymous"
    
    parts = name.strip().split()
    masked_parts = []
    for part in parts:
        if len(part) > 1:
            masked_parts.append(part[0] + "*" * (len(part) - 1))
        else:
            masked_parts.append(part)
            
    return " ".join(masked_parts)

def hash_email(email: str) -> str:
    """
    Gera um hash SHA-256 persistente a partir do e-mail do usuário combinado com um Salt.
    Atua como o ID Analítico Único para joins de produtos sem armazenar o e-mail real (PII).
    """
    if not email:
        return ""
    
    email_clean = email.strip().lower()
    salted_email = f"{email_clean}{ANALYTIC_SALT}"
    
    # Executa a criptografia hash de via única
    hasher = hashlib.sha256()
    hasher.update(salted_email.encode('utf-8'))
    return hasher.hexdigest()

def process_and_store_user(raw_event: dict):
    """
    Recebe os dados brutos de cadastro do backend, aplica transformações e higienização 
    conforme as regras da LGPD, e salva nos repositórios de dados analíticos (Parquet + DuckDB).
    """
    # 1. Higienização e Remoção de Senhas (garante que senhas NUNCA fiquem arquivadas no Data Lake)
    raw_event.pop("password", None)
    
    # 2. Extração e Mascaramento PII
    real_email = raw_event.get("email", "")
    real_name = raw_event.get("name", "")
    
    hashed_email_id = hash_email(real_email)
    masked_username = mask_name(real_name)
    
    # 3. Construção do Registro Analítico Higienizado (Conformidade com a LGPD)
    analytical_record = {
        "user_id": raw_event.get("userId"),
        "hashed_email_id": hashed_email_id,       # Join Key Analítica Segura
        "masked_name": masked_username,           # Nome Pseudonimizado
        "provider": raw_event.get("provider", "LOCAL"),
        "created_at": raw_event.get("createdAt"),
        "ingested_at": datetime.utcnow().isoformat()
    }
    
    print(f"[DATA PLATFORM] Processando PII para usuário {analytical_record['user_id']}:")
    print(f"  -> Nome original mascarado: {real_name} -> {masked_username}")
    print(f"  -> E-mail original hasheado: {real_email} -> {hashed_email_id[:12]}...")
    
    # --- PERSISTÊNCIA 1: Datasets em Formato Parquet (Data Lake) ---
    df_new = pd.DataFrame([analytical_record])
    
    if os.path.exists(PARQUET_FILE):
        try:
            df_existing = pd.read_parquet(PARQUET_FILE)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_parquet(PARQUET_FILE, index=False)
        except Exception as e:
            print(f"[DATA PLATFORM] Erro ao concatenar Parquet existente: {e}. Sobrescrevendo...")
            df_new.to_parquet(PARQUET_FILE, index=False)
    else:
        df_new.to_parquet(PARQUET_FILE, index=False)
        
    print(f"[DATA PLATFORM] Registro adicionado ao arquivo Parquet: {PARQUET_FILE}")
    
    # --- PERSISTÊNCIA 2: Data Warehouse SQL (DuckDB) ---
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        
        # Criação da tabela analítica se não existir
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users_analytical (
                user_id VARCHAR PRIMARY KEY,
                hashed_email_id VARCHAR,
                masked_name VARCHAR,
                provider VARCHAR,
                created_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
        """)
        
        # Inserção segura via prepared statement
        conn.execute("""
            INSERT OR REPLACE INTO users_analytical 
            (user_id, hashed_email_id, masked_name, provider, created_at, ingested_at) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            analytical_record["user_id"],
            analytical_record["hashed_email_id"],
            analytical_record["masked_name"],
            analytical_record["provider"],
            analytical_record["created_at"],
            analytical_record["ingested_at"]
        ])
        
        # Print resumido do total de registros para auditoria de dados
        count = conn.execute("SELECT COUNT(*) FROM users_analytical").fetchone()[0]
        conn.close()
        
        print(f"[DATA PLATFORM] Registro salvo no DuckDB: {DUCKDB_FILE}. Total na tabela: {count}")
        
    except Exception as e:
        print(f"[DATA PLATFORM] Erro ao persistir dados no DuckDB warehouse: {e}")

def purge_analytical_data(emails: list[str] = None, purge_all: bool = False) -> dict:
    """
    Remove registros do Data Lake (Parquet) e do Data Warehouse (DuckDB).
    - Se purge_all for True, remove todos os dados.
    - Se emails for fornecido, remove apenas os registros correspondentes aos hashes desses e-mails.
    """
    if purge_all:
        # 1. Parquet
        if os.path.exists(PARQUET_FILE):
            try:
                os.remove(PARQUET_FILE)
                print("[DATA PLATFORM] Arquivo Parquet deletado (todos os dados).")
            except Exception as e:
                print(f"[DATA PLATFORM] Erro ao deletar arquivo Parquet: {e}")
        
        # 2. DuckDB
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            conn.execute("DROP TABLE IF EXISTS users_analytical")
            conn.close()
            print("[DATA PLATFORM] Tabela users_analytical dropada do DuckDB.")
        except Exception as e:
            print(f"[DATA PLATFORM] Erro ao limpar DuckDB: {e}")
        return {"status": "success", "message": "Todos os dados analíticos foram removidos."}

    if emails:
        # Calcular os hashes correspondentes utilizando o salt
        hashes_to_remove = [hash_email(email) for email in emails if email]
        if not hashes_to_remove:
            return {"status": "success", "message": "Nenhum e-mail válido fornecido para remoção."}
            
        # 1. Parquet
        if os.path.exists(PARQUET_FILE):
            try:
                df = pd.read_parquet(PARQUET_FILE)
                df_filtered = df[~df['hashed_email_id'].isin(hashes_to_remove)]
                df_filtered.to_parquet(PARQUET_FILE, index=False)
                print(f"[DATA PLATFORM] Registros de e-mails específicos removidos do Parquet. Linhas antes: {len(df)}, depois: {len(df_filtered)}")
            except Exception as e:
                print(f"[DATA PLATFORM] Erro ao filtrar Parquet: {e}")
                
        # 2. DuckDB
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            table_exists = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'users_analytical'").fetchone()[0] > 0
            if table_exists:
                placeholders = ', '.join(['?'] * len(hashes_to_remove))
                query = f"DELETE FROM users_analytical WHERE hashed_email_id IN ({placeholders})"
                conn.execute(query, hashes_to_remove)
                count = conn.execute("SELECT COUNT(*) FROM users_analytical").fetchone()[0]
                print(f"[DATA PLATFORM] Registros removidos do DuckDB. Novo total: {count}")
            conn.close()
        except Exception as e:
            print(f"[DATA PLATFORM] Erro ao deletar e-mails do DuckDB: {e}")
            
        return {"status": "success", "message": "Dados dos e-mails especificados foram removidos."}
        
    return {"status": "error", "message": "Nenhum parâmetro de remoção válido fornecido."}


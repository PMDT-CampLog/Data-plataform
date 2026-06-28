"""
SearchIndexerWorker — Worker CDC para indexação de perfis no motor de busca.

DECISÃO ARQUITETURAL:
Este worker simula o consumo de eventos CDC (Change Data Capture) do módulo
de conexões para manter um índice de busca textual atualizado. Em produção,
seria conectado a uma fila (SQS, RabbitMQ, Kafka) ou stream de CDC (Debezium).

ESTRUTURA DO ÍNDICE:
O payload de indexação é compatível com Typesense e Elasticsearch,
otimizado para:
- Busca textual rápida por display_name e bio
- Facets por profile_type (CREATOR/SUPPORTER) e tags
- Sorting por followers_count (relevância social)
- Atualização incremental de followers_count via CDC sem reindexação completa

CONFORMIDADE LGPD:
Nenhum dado sensível (e-mail, senha) é indexado. Apenas dados públicos
de perfil são armazenados no índice de busca.
"""

import os
import json
import duckdb
from datetime import datetime, timezone
from typing import Optional

# Diretório do Data Lake — reaproveitado do pipeline existente
DATA_LAKE_DIR = os.path.join(os.path.dirname(__file__), "data_lake")
DUCKDB_FILE = os.path.join(DATA_LAKE_DIR, "camplog_warehouse.db")

# Garantir que o diretório existe
os.makedirs(DATA_LAKE_DIR, exist_ok=True)


# ===========================================================================
# Schema do Índice de Busca (compatível com Typesense/Elasticsearch)
# ===========================================================================

SEARCH_INDEX_SCHEMA = {
    "name": "profiles",
    "fields": [
        # Campo composto: {profile_type}_{profile_id} — garante unicidade cross-type
        {"name": "id", "type": "string"},

        # Campos de identificação
        {"name": "profile_id", "type": "string", "index": True},
        {"name": "profile_type", "type": "string", "facet": True},  # CREATOR | SUPPORTER

        # Campos de busca textual — os que o usuário pesquisa
        {"name": "display_name", "type": "string", "index": True, "sort": True},
        {"name": "bio", "type": "string", "index": True, "optional": True},

        # Dados de exibição nos resultados
        {"name": "avatar_url", "type": "string", "optional": True, "index": False},

        # Métricas sociais — atualizadas incrementalmente via CDC
        {"name": "followers_count", "type": "int32", "sort": True},
        {"name": "following_count", "type": "int32", "sort": True},

        # Tags para filtragem e facets
        {"name": "tags", "type": "string[]", "facet": True, "optional": True},

        # Timestamp de última atualização — usado para ordering por relevância temporal
        {"name": "updated_at", "type": "int64"},  # Unix timestamp em segundos
    ],
    # Campos que são buscados quando o usuário digita uma query
    "default_sorting_field": "followers_count",
    "token_separators": ["-", "_"],
}


class SearchIndexerWorker:
    """
    Worker que processa eventos CDC de conexões e atualiza o índice de busca.

    Em produção, esta classe seria um consumer de fila (ex: SQS, RabbitMQ).
    Na versão atual, usa DuckDB como armazenamento simulado do índice.
    """

    def __init__(self):
        """Inicializa o worker e cria a tabela do índice de busca no DuckDB."""
        self._ensure_search_index_table()
        print("[SEARCH INDEXER] Worker inicializado com sucesso.")

    def _ensure_search_index_table(self):
        """Cria a tabela do índice de busca se não existir."""
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS search_profiles_index (
                    id VARCHAR PRIMARY KEY,
                    profile_id VARCHAR NOT NULL,
                    profile_type VARCHAR NOT NULL,
                    display_name VARCHAR NOT NULL,
                    bio TEXT,
                    avatar_url VARCHAR,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    tags VARCHAR[],
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            conn.close()
            print("[SEARCH INDEXER] Tabela search_profiles_index garantida no DuckDB.")
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro ao criar tabela de índice: {e}")

    def process_follow_event(self, event_data: dict):
        """
        Processa um evento PROFILE_FOLLOWED do módulo de conexões.

        Ação: incrementa followers_count do perfil seguido.
        Se o perfil ainda não existe no índice, cria um registro mínimo
        que será enriquecido na próxima sincronização completa.

        Payload esperado (do backend Spring):
        {
            "eventType": "PROFILE_FOLLOWED",
            "followerId": "uuid-do-seguidor",
            "followerType": "SUPPORTER",
            "followedId": "uuid-do-seguido",
            "followedType": "CREATOR",
            "occurredAt": "2026-06-27T22:00:00"
        }
        """
        followed_id = event_data.get("followedId")
        followed_type = event_data.get("followedType")
        index_id = f"{followed_type}_{followed_id}"

        print(f"[SEARCH INDEXER] Processando PROFILE_FOLLOWED: {index_id}")

        try:
            conn = duckdb.connect(DUCKDB_FILE)

            # Verifica se o perfil já existe no índice
            existing = conn.execute(
                "SELECT id FROM search_profiles_index WHERE id = ?",
                [index_id]
            ).fetchone()

            if existing:
                # Atualização incremental — apenas incrementa followers_count
                conn.execute("""
                    UPDATE search_profiles_index 
                    SET followers_count = followers_count + 1,
                        updated_at = ?
                    WHERE id = ?
                """, [datetime.now(timezone.utc).isoformat(), index_id])
                print(f"[SEARCH INDEXER] followers_count incrementado para {index_id}")
            else:
                # Perfil não encontrado no índice — cria registro mínimo
                conn.execute("""
                    INSERT INTO search_profiles_index 
                    (id, profile_id, profile_type, display_name, followers_count, following_count, updated_at)
                    VALUES (?, ?, ?, ?, 1, 0, ?)
                """, [
                    index_id,
                    followed_id,
                    followed_type,
                    "Perfil pendente",  # Será enriquecido na próxima sync
                    datetime.now(timezone.utc).isoformat()
                ])
                print(f"[SEARCH INDEXER] Registro mínimo criado para {index_id} (pendente de enriquecimento)")

            conn.close()
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro ao processar follow event: {e}")

    def process_unfollow_event(self, event_data: dict):
        """
        Processa um evento PROFILE_UNFOLLOWED.
        Ação: decrementa followers_count (mínimo 0).
        """
        followed_id = event_data.get("followedId")
        followed_type = event_data.get("followedType")
        index_id = f"{followed_type}_{followed_id}"

        print(f"[SEARCH INDEXER] Processando PROFILE_UNFOLLOWED: {index_id}")

        try:
            conn = duckdb.connect(DUCKDB_FILE)
            conn.execute("""
                UPDATE search_profiles_index 
                SET followers_count = GREATEST(followers_count - 1, 0),
                    updated_at = ?
                WHERE id = ?
            """, [datetime.now(timezone.utc).isoformat(), index_id])
            conn.close()
            print(f"[SEARCH INDEXER] followers_count decrementado para {index_id}")
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro ao processar unfollow event: {e}")

    def upsert_profile(
        self,
        profile_id: str,
        profile_type: str,
        display_name: str,
        bio: Optional[str] = None,
        avatar_url: Optional[str] = None,
        tags: Optional[list[str]] = None,
    ):
        """
        Indexa ou atualiza um perfil completo no índice de busca.

        Usado em dois cenários:
        1. Sincronização batch (cron) — reindexação de todos os perfis
        2. Enriquecimento de registros mínimos criados por eventos CDC

        O payload gerado é compatível com a API do Typesense:
        POST /collections/profiles/documents
        """
        index_id = f"{profile_type}_{profile_id}"

        # Payload otimizado para busca textual — o que seria enviado ao Typesense
        indexed_payload = {
            "id": index_id,
            "profile_id": profile_id,
            "profile_type": profile_type,
            "display_name": display_name,
            "bio": bio or "",
            "avatar_url": avatar_url,
            "followers_count": 0,  # Será atualizado por CDC
            "following_count": 0,
            "tags": tags or [],
            "updated_at": int(datetime.now(timezone.utc).timestamp()),
        }

        print(f"[SEARCH INDEXER] Indexando perfil: {json.dumps(indexed_payload, indent=2, ensure_ascii=False)}")

        try:
            conn = duckdb.connect(DUCKDB_FILE)

            # Preserva contadores existentes se o perfil já estava indexado
            existing = conn.execute(
                "SELECT followers_count, following_count FROM search_profiles_index WHERE id = ?",
                [index_id]
            ).fetchone()

            if existing:
                followers_count, following_count = existing
                conn.execute("""
                    UPDATE search_profiles_index 
                    SET display_name = ?, bio = ?, avatar_url = ?,
                        tags = ?, updated_at = ?,
                        followers_count = ?, following_count = ?
                    WHERE id = ?
                """, [
                    display_name, bio, avatar_url,
                    tags or [], datetime.now(timezone.utc).isoformat(),
                    followers_count, following_count,
                    index_id
                ])
                print(f"[SEARCH INDEXER] Perfil atualizado: {index_id} (contadores preservados)")
            else:
                conn.execute("""
                    INSERT INTO search_profiles_index 
                    (id, profile_id, profile_type, display_name, bio, avatar_url,
                     followers_count, following_count, tags, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
                """, [
                    index_id, profile_id, profile_type,
                    display_name, bio, avatar_url,
                    tags or [], datetime.now(timezone.utc).isoformat()
                ])
                print(f"[SEARCH INDEXER] Novo perfil indexado: {index_id}")

            conn.close()
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro ao indexar perfil: {e}")

    def search_profiles(self, query: str, profile_type: Optional[str] = None, limit: int = 10) -> list[dict]:
        """
        Busca textual simulada no índice de perfis.

        Em produção, esta query seria delegada ao Typesense/Elasticsearch:
        GET /collections/profiles/documents/search?q={query}&query_by=display_name,bio

        A busca DuckDB simula o comportamento com ILIKE.
        """
        print(f"[SEARCH INDEXER] Buscando perfis: query='{query}', type={profile_type}, limit={limit}")

        try:
            conn = duckdb.connect(DUCKDB_FILE)

            sql = """
                SELECT profile_id, profile_type, display_name, bio, avatar_url,
                       followers_count, following_count, tags, updated_at
                FROM search_profiles_index
                WHERE (display_name ILIKE ? OR bio ILIKE ?)
            """
            params = [f"%{query}%", f"%{query}%"]

            if profile_type:
                sql += " AND profile_type = ?"
                params.append(profile_type)

            sql += " ORDER BY followers_count DESC LIMIT ?"
            params.append(limit)

            results = conn.execute(sql, params).fetchall()
            conn.close()

            columns = [
                "profile_id", "profile_type", "display_name", "bio",
                "avatar_url", "followers_count", "following_count", "tags", "updated_at"
            ]

            profiles = [dict(zip(columns, row)) for row in results]
            print(f"[SEARCH INDEXER] {len(profiles)} perfil(is) encontrado(s) para '{query}'")
            return profiles
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro na busca: {e}")
            return []

    def get_index_stats(self) -> dict:
        """Retorna estatísticas do índice de busca."""
        try:
            conn = duckdb.connect(DUCKDB_FILE)
            total = conn.execute("SELECT COUNT(*) FROM search_profiles_index").fetchone()[0]
            creators = conn.execute(
                "SELECT COUNT(*) FROM search_profiles_index WHERE profile_type = 'CREATOR'"
            ).fetchone()[0]
            supporters = conn.execute(
                "SELECT COUNT(*) FROM search_profiles_index WHERE profile_type = 'SUPPORTER'"
            ).fetchone()[0]
            conn.close()

            return {
                "total_indexed": total,
                "creators": creators,
                "supporters": supporters,
                "index_name": "profiles",
                "schema_version": "1.0.0"
            }
        except Exception as e:
            print(f"[SEARCH INDEXER] Erro ao obter stats: {e}")
            return {"error": str(e)}


# ===========================================================================
# Exemplo de uso demonstrativo (executável diretamente)
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  CampLog Search Indexer — Demonstração CDC")
    print("=" * 60)

    worker = SearchIndexerWorker()

    # 1. Indexar perfis de exemplo
    worker.upsert_profile(
        profile_id="creator-uuid-001",
        profile_type="CREATOR",
        display_name="Matheus Araújo",
        bio="Desenvolvedor apaixonado por jogos indie e web",
        avatar_url="https://cdn.camplog.dev/avatars/creator-001.webp",
        tags=["gamedev", "indie", "webdev"],
    )

    worker.upsert_profile(
        profile_id="supporter-uuid-002",
        profile_type="SUPPORTER",
        display_name="Ana Silva",
        bio="Entusiasta de tecnologia e apoiadora de criadores",
        avatar_url="https://cdn.camplog.dev/avatars/supporter-002.webp",
        tags=["tech", "community"],
    )

    # 2. Simular evento CDC de follow
    follow_event = {
        "eventType": "PROFILE_FOLLOWED",
        "followerId": "supporter-uuid-002",
        "followerType": "SUPPORTER",
        "followedId": "creator-uuid-001",
        "followedType": "CREATOR",
        "occurredAt": datetime.now(timezone.utc).isoformat(),
    }
    worker.process_follow_event(follow_event)

    # 3. Buscar perfis
    results = worker.search_profiles("Matheus")
    print(f"\nResultados da busca por 'Matheus':")
    print(json.dumps(results, indent=2, ensure_ascii=False, default=str))

    # 4. Estatísticas do índice
    stats = worker.get_index_stats()
    print(f"\nEstatísticas do índice:")
    print(json.dumps(stats, indent=2))

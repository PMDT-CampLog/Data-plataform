import os
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks, status
from pydantic import BaseModel, EmailStr
from pipeline import process_and_store_user, purge_analytical_data, process_spotify_event
from search_indexer import SearchIndexerWorker
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="CampLog Data Platform Ingestion API",
    description="API de alta performance para ingestão de eventos e telemetria analítica com conformidade LGPD.",
    version="1.0.0"
)

# Instrumentação de Métricas do Prometheus
Instrumentator().instrument(app).expose(app)

# Chave secreta de autenticação entre microsserviços
SECRET_SIGNATURE = os.getenv("DATA_PLATFORM_SECRET", "camp-log-data-sec-123")

# Schema de Validação de Dados recebido do Backend (Pydantic v2)
class UserCreatedPayload(BaseModel):
    userId: str
    name: str
    email: EmailStr
    provider: str
    createdAt: str

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """
    Verificação de saúde (Health Check) do microsserviço do Data Platform.
    """
    return {"status": "healthy", "service": "camplog-data-platform"}

@app.post("/api/v1/events/user-created", status_code=status.HTTP_202_ACCEPTED)
def ingest_user_created_event(
    payload: UserCreatedPayload,
    background_tasks: BackgroundTasks,
    x_camplog_signature: str = Header(None)
):
    """
    Recebe os eventos assíncronos 'USER_CREATED' disparados pelo backend REST,
    valida a assinatura de autenticação e delega para o pipeline analítico de forma não bloqueante.
    """
    # 1. Validação de Assinatura de Segurança (Shared Secret)
    if not x_camplog_signature or x_camplog_signature != SECRET_SIGNATURE:
        print(f"[DATA PLATFORM - AUTH FAILED] Assinatura incorreta recebida: {x_camplog_signature}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assinatura X-CampLog-Signature ausente ou inválida."
        )

    print(f"[DATA PLATFORM] Evento de cadastro recebido com sucesso para o ID: {payload.userId}")

    # 2. Execução não-bloqueante via BackgroundTasks do FastAPI
    # Isso desonera a thread do servidor de responder imediatamente ao webhook do Backend
    background_tasks.add_task(process_and_store_user, payload.model_dump())

    return {
        "status": "success",
        "message": "Evento aceito com sucesso e agendado para higienização e persistência no Data Lake."
    }

class SpotifyEventPayload(BaseModel):
    userId: str
    eventType: str # LINKED, UNLINKED, TRACK_PINNED
    trackId: str | None = None
    timestamp: str

@app.post("/api/v1/events/spotify", status_code=status.HTTP_202_ACCEPTED)
def ingest_spotify_event(
    payload: SpotifyEventPayload,
    background_tasks: BackgroundTasks,
    x_camplog_signature: str = Header(None)
):
    """
    Recebe os eventos assíncronos do Spotify (link, unlink, música fixada) disparados pelo backend.
    """
    if not x_camplog_signature or x_camplog_signature != SECRET_SIGNATURE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assinatura X-CampLog-Signature ausente ou inválida."
        )

    print(f"[DATA PLATFORM] Evento Spotify ({payload.eventType}) recebido para o ID: {payload.userId}")

    background_tasks.add_task(process_spotify_event, payload.model_dump())
    
    return {
        "status": "success",
        "message": "Evento Spotify aceito com sucesso."
    }

class PurgePayload(BaseModel):
    emails: list[EmailStr] | None = None
    all: bool | None = False

@app.delete("/api/v1/events/users/purge", status_code=status.HTTP_200_OK)
def purge_users_data(
    payload: PurgePayload,
    x_camplog_signature: str = Header(None)
):
    """
    Remove registros analíticos (LGPD) de todos os usuários ou de e-mails específicos.
    """
    if not x_camplog_signature or x_camplog_signature != SECRET_SIGNATURE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assinatura X-CampLog-Signature ausente ou inválida."
        )

    result = purge_analytical_data(emails=payload.emails, purge_all=payload.all)
    if result.get("status") == "error":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.get("message"))

    return result

from fastapi.staticfiles import StaticFiles
from storage import handle_upload, LOCAL_UPLOAD_DIR
from fastapi import UploadFile, File

if os.getenv("ENVIRONMENT", "local") == "local":
    app.mount("/uploads", StaticFiles(directory=LOCAL_UPLOAD_DIR), name="uploads")

@app.post("/api/v1/storage/upload", status_code=status.HTTP_201_CREATED)
def upload_media(
    file: UploadFile = File(...),
    folder: str = "profiles",
    x_camplog_signature: str = Header(None)
):
    """
    Endpoint para envio de arquivos de mídia (imagens de perfil, banners).
    Salva localmente ou S3 dependendo do ambiente.
    """
    if not x_camplog_signature or x_camplog_signature != SECRET_SIGNATURE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assinatura X-CampLog-Signature ausente ou inválida."
        )
        
    url = handle_upload(file, folder=folder)
    if not url:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Falha no upload do arquivo.")
        
    return {"status": "success", "url": url}

# ===========================================================================
# Ingestão de Eventos de Conexões (Follow/Unfollow) para o Search Indexer
# ===========================================================================

# Instância do worker de indexação — inicializada uma única vez
search_indexer = SearchIndexerWorker()

class ProfileConnectionEventPayload(BaseModel):
    """Payload do evento de conexão enviado pelo backend Spring."""
    eventType: str  # PROFILE_FOLLOWED | PROFILE_UNFOLLOWED
    followerId: str
    followerType: str  # CREATOR | SUPPORTER
    followedId: str
    followedType: str  # CREATOR | SUPPORTER
    occurredAt: str

@app.post("/api/v1/events/profile-connection", status_code=status.HTTP_202_ACCEPTED)
def ingest_profile_connection_event(
    payload: ProfileConnectionEventPayload,
    background_tasks: BackgroundTasks,
    x_camplog_signature: str = Header(None)
):
    """
    Recebe eventos de conexão (follow/unfollow) do backend e delega
    ao SearchIndexerWorker para atualização do índice de busca em near-real-time.
    """
    if not x_camplog_signature or x_camplog_signature != SECRET_SIGNATURE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assinatura X-CampLog-Signature ausente ou inválida."
        )

    print(f"[DATA PLATFORM] Evento de conexão ({payload.eventType}) recebido: "
          f"{payload.followerType}:{payload.followerId} → {payload.followedType}:{payload.followedId}")

    event_data = payload.model_dump()

    if payload.eventType == "PROFILE_FOLLOWED":
        background_tasks.add_task(search_indexer.process_follow_event, event_data)
    elif payload.eventType == "PROFILE_UNFOLLOWED":
        background_tasks.add_task(search_indexer.process_unfollow_event, event_data)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tipo de evento desconhecido: {payload.eventType}"
        )

    return {
        "status": "success",
        "message": f"Evento {payload.eventType} aceito para indexação."
    }


if __name__ == "__main__":
    import uvicorn
    # Inicialização direta do servidor web para testes
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

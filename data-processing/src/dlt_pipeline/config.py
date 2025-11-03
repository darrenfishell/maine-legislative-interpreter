import os
from pathlib import Path


class Config:
    """Central configuration with env-driven overrides."""

    # Database
    DB_NAME: str = os.getenv('MLI_DB_NAME', 'maine-legislative-testimony')
    BRONZE_SCHEMA: str = os.getenv('MLI_BRONZE_SCHEMA', 'bronze')
    SILVER_SCHEMA: str = os.getenv('MLI_SILVER_SCHEMA', 'silver')

    # Embeddings
    EMBEDDING_MODEL: str = os.getenv('MLI_EMBEDDING_MODEL', 'all-MiniLM-L12-v2')
    CPU_BATCH_SIZE: int = int(os.getenv('MLI_EMBED_CPU_BATCH', '64'))
    MPS_BATCH_SIZE: int = int(os.getenv('MLI_EMBED_MPS_BATCH', '128'))

    # Paths
    BASE_DIR: Path = Path(__file__).resolve().parents[1]
    PDF_DIR: Path = Path(os.getenv('MLI_PDF_DIR', BASE_DIR / 'testimony_pdfs'))

    # Logging / errors
    LOG_LEVEL: str = os.getenv('MLI_LOG_LEVEL', 'INFO')
    QUIET_ERRORS: bool = os.getenv('MLI_QUIET_ERRORS', '1') not in ('0', 'false', 'False')

    # Retry/backoff
    RETRY_ATTEMPTS: int = int(os.getenv('MLI_RETRY_ATTEMPTS', '3'))
    RETRY_BACKOFF_SEC: float = float(os.getenv('MLI_RETRY_BACKOFF', '0.5'))



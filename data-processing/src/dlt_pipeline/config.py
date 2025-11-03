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



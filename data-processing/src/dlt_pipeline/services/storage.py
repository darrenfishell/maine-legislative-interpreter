import os
from pathlib import Path
from ..config import Config


def get_pdf_repo(session: int) -> Path:
    """Return the directory path for storing PDFs for a given session.

    Ensures the directory exists.
    """
    pdf_repo = Path(Config.PDF_DIR) / str(session)
    os.makedirs(pdf_repo, exist_ok=True)
    return pdf_repo



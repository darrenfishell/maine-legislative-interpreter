from typing import Optional
from pypdf import PdfReader
from ..config import Config
from ..utils.concurrency import retry
from tqdm import tqdm


def read_pdf_text(filepath: str) -> Optional[str]:
    """Read text from a PDF file, returning a single string or None on failure."""

    def _do():
        pdf = PdfReader(filepath, strict=False)
        return ' '.join([page.extract_text() for page in pdf.pages]).strip() or None

    def _on_err(e: BaseException, attempt: int):
        if not Config.QUIET_ERRORS:
            tqdm.write(f"read_pdf_text failed for path={filepath} (attempt {attempt}): {e}")

    return retry(
        _do,
        exceptions=(Exception,),
        attempts=Config.RETRY_ATTEMPTS,
        backoff_sec=Config.RETRY_BACKOFF_SEC,
        on_error=_on_err,
    )



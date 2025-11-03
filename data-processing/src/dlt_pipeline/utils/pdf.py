from typing import Optional
from pypdf import PdfReader


def read_pdf_text(filepath: str) -> Optional[str]:
    """Read text from a PDF file, returning a single string or None on failure."""
    pdf = PdfReader(filepath, strict=False)
    raw_text = ' '.join([page.extract_text() for page in pdf.pages]).strip() or None
    return raw_text



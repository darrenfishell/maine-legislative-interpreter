import re
import unicodedata


def clean_text(text: str) -> str:
    """Clean PDF-extracted text for downstream sentence splitting and embeddings.

    The function mirrors the existing logic in dlt_sources while keeping it
    pure and unit-testable.
    """
    # Convert to string if it's not already
    if not isinstance(text, str):
        text = str(text)

    # Decode common Unicode escape sequences
    text = text.encode('utf-8', errors='ignore').decode('utf-8')

    # Handle common PDF extraction artifacts
    text = re.sub(r'u[0-9a-fA-F]{4}', '', text)  # Remove u0000, u2018, etc.
    text = re.sub(r'\\u[0-9a-fA-F]{4}', '', text)  # Remove escaped unicode
    text = re.sub(r'\\x[0-9a-fA-F]{2}', '', text)  # Remove hex escapes

    # Remove control characters and non-printable characters
    text = ''.join(char for char in text if unicodedata.category(char)[0] != 'C')

    # Normalize Unicode characters
    text = unicodedata.normalize('NFKC', text)

    # Replace common problematic characters
    replacements = {
        '\u2018': "'",
        '\u2019': "'",
        '\u201c': '"',
        '\u201d': '"',
        '\u2013': '-',
        '\u2014': '--',
        '\u2022': '•',
        '\u00a0': ' ',
        '\u00b0': '°',
        '\u00ae': '®',
        '\u00a7': '§',
        '\u00bb': '»',
        '\u00ab': '«',
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Remove excessive whitespace and normalize line breaks
    text = re.sub(r'\n+|\\n|\r\n|\r', ' ', text)
    text = re.sub(r'\t+', ' ', text)
    text = re.sub(r'\s+', ' ', text)

    # Remove common PDF artifacts
    text = re.sub(r'[^\w\s.,!?;:()\'"\-–—•°®§»«]', '', text)

    # Clean up punctuation
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:])\s*([.,!?;:])', r'\1', text)

    # Remove isolated characters and very short fragments
    text = re.sub(r'\b[a-zA-Z]\b', '', text)
    text = re.sub(r'\b\d+\b', '', text)

    # Final cleanup
    text = text.strip()

    return text



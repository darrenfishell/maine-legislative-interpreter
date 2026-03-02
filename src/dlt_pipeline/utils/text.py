import re
import unicodedata
import ftfy


def fix_encoding(text: str) -> str:
    """Repair mojibake, broken Unicode escapes, and encoding mix-ups."""
    text = ftfy.fix_text(text)
    text = text.encode('utf-8', errors='ignore').decode('utf-8')
    return text


def remove_control_characters(text: str) -> str:
    """Strip non-printable control characters while preserving newlines and tabs."""
    return ''.join(
        char for char in text
        if unicodedata.category(char)[0] != 'C' or char in ('\n', '\t')
    )


def normalize_unicode(text: str) -> str:
    """Apply NFKC normalization and replace common typographic variants."""
    text = unicodedata.normalize('NFKC', text)
    replacements = {
        '\u2018': "'",   # left single quote
        '\u2019': "'",   # right single quote
        '\u201c': '"',   # left double quote
        '\u201d': '"',   # right double quote
        '\u2013': '-',   # en dash
        '\u2014': '--',  # em dash
        '\u2022': '-',   # bullet -> hyphen
        '\u00a0': ' ',   # non-breaking space
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def remove_pdf_artifacts(text: str) -> str:
    """Remove common PDF extraction artifacts using a blocklist approach."""
    # Null bytes and replacement characters
    text = text.replace('\x00', '')
    text = text.replace('\ufffd', '')

    # Escaped unicode/hex sequences that survived extraction (literal backslash)
    text = re.sub(r'\\u[0-9a-fA-F]{4}', '', text)
    text = re.sub(r'\\x[0-9a-fA-F]{2}', '', text)

    # Page numbers on their own line
    text = re.sub(r'(?m)^\s*Page\s+\d+\s*(?:of\s+\d+)?\s*$', '', text)
    text = re.sub(r'(?m)^\s*-\s*\d+\s*-\s*$', '', text)

    # Repeated header/footer-like lines (very short lines that repeat)
    text = re.sub(r'(?m)^\s*\d+\s*$', '', text)

    # Form feed characters
    text = text.replace('\f', '\n')

    return text


def normalize_whitespace(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    text = re.sub(r'\r\n|\r', '\n', text)
    text = re.sub(r'\t+', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' *\n *', '\n', text)
    return text


def clean_punctuation(text: str) -> str:
    """Fix spacing around punctuation."""
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:]){3,}', r'\1\1', text)
    return text


def clean_text(text: str) -> str:
    """Clean PDF-extracted text for downstream sentence splitting and embeddings.

    Applies a pipeline of composable cleaning steps:
    1. Encoding repair (ftfy)
    2. Control character removal
    3. Unicode normalization
    4. PDF artifact removal (blocklist, not allowlist)
    5. Whitespace normalization
    6. Punctuation cleanup
    """
    if not isinstance(text, str):
        text = str(text)

    if not text.strip():
        return ''

    text = fix_encoding(text)
    text = remove_control_characters(text)
    text = normalize_unicode(text)
    text = remove_pdf_artifacts(text)
    text = normalize_whitespace(text)
    text = clean_punctuation(text)
    text = text.strip()

    return text

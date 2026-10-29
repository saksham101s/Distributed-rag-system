import io
import logging
import os
import re
from typing import Optional

logger = logging.getLogger("TextExtraction")

TEXT_EXTENSIONS = {".txt", ".md", ".markdown", ".rst", ".csv", ".log"}
PDF_EXTENSIONS = {".pdf"}
SUPPORTED_EXTENSIONS = TEXT_EXTENSIONS | PDF_EXTENSIONS


class UnsupportedFileType(ValueError):
    """Raised when an uploaded file can't be turned into text."""


def _decode_text(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16"):
        try:
            text = data.decode(encoding)
            if encoding == "utf-16" and "\x00" in text:
                continue
            return text
        except UnicodeDecodeError:
            continue
    # Last resort: keep what decodes, so one bad byte doesn't reject the whole file
    return data.decode("utf-8", errors="replace")


def _extract_pdf(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception as e:
            raise UnsupportedFileType("PDF is password protected") from e
    pages = [(page.extract_text() or "").strip() for page in reader.pages]
    return "\n\n".join(p for p in pages if p)


def extract_text(filename: str, data: bytes, content_type: Optional[str] = None) -> str:
    """Returns the plain text of an uploaded .txt/.md/.pdf (etc.) file, with whitespace normalized."""
    ext = os.path.splitext(filename or "")[1].lower()
    if ext in PDF_EXTENSIONS or content_type == "application/pdf" or data[:5] == b"%PDF-":
        text = _extract_pdf(data)
    elif ext in TEXT_EXTENSIONS or (content_type or "").startswith("text/"):
        text = _decode_text(data)
    else:
        raise UnsupportedFileType(
            f"Unsupported file type '{ext or content_type}'. Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )

    text = text.replace("\r\n", "\n").replace("\x00", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def doc_id_from_filename(filename: str) -> str:
    """Builds a stable doc_id from a filename: 'My Notes (v2).pdf' -> 'my-notes-v2'."""
    stem = os.path.splitext(os.path.basename(filename or ""))[0].lower()
    slug = re.sub(r"[^a-z0-9]+", "-", stem).strip("-")
    return slug[:200] or "document"

import uuid
import hashlib
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class DocumentChunk(BaseModel):
    """Represents a single chunk of text extracted from a larger document."""
    chunk_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_id: str
    text: str
    chunk_index: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None

    def get_hash(self) -> str:
        """Returns deterministic hash of chunk content."""
        return hashlib.md5(f"{self.doc_id}_{self.chunk_index}_{self.text[:50]}".encode()).hexdigest()


class DocumentProcessor:
    """Handles splitting raw text documents into manageable overlapping chunks."""
    
    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def process_document(
        self, 
        doc_id: str, 
        text: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[DocumentChunk]:
        """Splits document text into overlapping chunks with metadata."""
        if metadata is None:
            metadata = {}
            
        chunks = []
        clean_text = text.strip()
        if not clean_text:
            return chunks

        start = 0
        text_length = len(clean_text)
        chunk_index = 0

        while start < text_length:
            end = min(start + self.chunk_size, text_length)
            
            # Adjust end to nearest space boundary if possible to avoid breaking words
            if end < text_length:
                last_space = clean_text.rfind(' ', start, end)
                if last_space != -1 and last_space > start + self.chunk_overlap:
                    end = last_space

            chunk_text = clean_text[start:end].strip()
            if chunk_text:
                chunk = DocumentChunk(
                    doc_id=doc_id,
                    text=chunk_text,
                    chunk_index=chunk_index,
                    metadata={**metadata, "doc_id": doc_id, "chunk_index": chunk_index}
                )
                chunks.append(chunk)
                chunk_index += 1

            if end == text_length:
                break
                
            start = max(start + 1, end - self.chunk_overlap)

        return chunks

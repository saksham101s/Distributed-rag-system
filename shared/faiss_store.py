import os
import faiss
import threading
import logging
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from pydantic import BaseModel
from shared.document_processor import DocumentChunk

logger = logging.getLogger("FAISSVectorStore")


class SearchResult(BaseModel):
    """Represents a vector similarity search result."""
    chunk_id: str
    doc_id: str
    text: str
    score: float
    chunk_index: int
    metadata: Dict[str, Any]
    shard_id: Optional[str] = None


class FAISSVectorStore:
    """In-memory FAISS index wrapper with custom ID mapping and metadata storage."""

    def __init__(self, dim: int = 384, shard_id: str = "shard_standalone"):
        self.dim = dim
        self.shard_id = shard_id
        self._lock = threading.RLock()
        
        # Base inner product index (exact inner product = cosine similarity for normalized vectors)
        base_index = faiss.IndexFlatIP(self.dim)
        # Wrap with IndexIDMap2 to allow explicit integer ID assignment and retrieval
        self.index = faiss.IndexIDMap2(base_index)
        
        # Internal mapping: int_id -> DocumentChunk
        self.id_to_chunk: Dict[int, DocumentChunk] = {}
        # Tracking: chunk_id -> int_id
        self.chunk_id_to_int: Dict[str, int] = {}
        self._next_id: int = 1

    def add_chunks(self, chunks: List[DocumentChunk], embeddings: np.ndarray) -> List[int]:
        """Adds document chunks and corresponding embedding vectors to FAISS index."""
        if not chunks:
            return []
        
        if len(chunks) != len(embeddings):
            raise ValueError(f"Mismatch between chunks ({len(chunks)}) and embeddings ({len(embeddings)})")

        with self._lock:
            assigned_ids = []
            vectors_to_add = []
            
            for i, chunk in enumerate(chunks):
                int_id = self._next_id
                self._next_id += 1
                
                assigned_ids.append(int_id)
                self.id_to_chunk[int_id] = chunk
                self.chunk_id_to_int[chunk.chunk_id] = int_id
                vectors_to_add.append(embeddings[i])

            vec_np = np.array(vectors_to_add, dtype=np.float32)
            id_np = np.array(assigned_ids, dtype=np.int64)
            
            self.index.add_with_ids(vec_np, id_np)
            logger.info(f"[{self.shard_id}] Added {len(chunks)} vectors to FAISS index. Total count: {self.index.ntotal}")
            return assigned_ids

    def search(self, query_vector: np.ndarray, top_k: int = 5) -> List[SearchResult]:
        """Searches FAISS index for top-k most similar chunks by cosine similarity."""
        with self._lock:
            if self.index.ntotal == 0:
                return []
            
            # Ensure query vector shape (1, dim) and float32
            if query_vector.ndim == 1:
                query_vector = np.expand_dims(query_vector, axis=0)
            query_vector = query_vector.astype(np.float32)

            k = min(top_k, self.index.ntotal)
            scores, ids = self.index.search(query_vector, k)

            results = []
            for score, int_id in zip(scores[0], ids[0]):
                if int_id == -1 or int_id not in self.id_to_chunk:
                    continue
                chunk = self.id_to_chunk[int_id]
                results.append(SearchResult(
                    chunk_id=chunk.chunk_id,
                    doc_id=chunk.doc_id,
                    text=chunk.text,
                    score=float(score),
                    chunk_index=chunk.chunk_index,
                    metadata=chunk.metadata,
                    shard_id=self.shard_id
                ))
            return results

    def delete_by_doc_id(self, doc_id: str) -> int:
        """Removes all chunks associated with a specific doc_id."""
        with self._lock:
            target_ids = [int_id for int_id, chunk in self.id_to_chunk.items() if chunk.doc_id == doc_id]
            if not target_ids:
                return 0
            
            id_selector = faiss.IDSelectorBatch(np.array(target_ids, dtype=np.int64))
            self.index.remove_ids(id_selector)

            for int_id in target_ids:
                chunk = self.id_to_chunk.pop(int_id, None)
                if chunk and chunk.chunk_id in self.chunk_id_to_int:
                    del self.chunk_id_to_int[chunk.chunk_id]
                    
            logger.info(f"[{self.shard_id}] Removed {len(target_ids)} vectors for doc_id {doc_id}")
            return len(target_ids)

    def count(self) -> int:
        """Returns total vector count in index."""
        with self._lock:
            return self.index.ntotal

    def clear(self):
        """Clears all vectors and metadata."""
        with self._lock:
            self.index.reset()
            self.id_to_chunk.clear()
            self.chunk_id_to_int.clear()
            self._next_id = 1

import faiss
import threading
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
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
    doc_version: int = 0
    replica_shards: List[str] = []  # other shards that returned this same chunk (set by the coordinator merge)


class DocumentInfo(BaseModel):
    """Per-document bookkeeping kept alongside the index."""
    doc_id: str
    version: int = 0
    replication_factor: int = 0
    chunk_count: int = 0
    metadata: Dict[str, Any] = {}


class FAISSVectorStore:
    """In-memory FAISS index wrapper with custom ID mapping, per-document versions and metadata storage."""

    def __init__(self, dim: int = 384, shard_id: str = "shard_standalone"):
        self.dim = dim
        self.shard_id = shard_id
        self._lock = threading.RLock()

        # Base inner product index (exact inner product = cosine similarity for normalized vectors)
        base_index = faiss.IndexFlatIP(self.dim)
        # Wrap with IndexIDMap2 to allow explicit integer ID assignment and reconstruction by ID
        self.index = faiss.IndexIDMap2(base_index)

        # Internal mapping: int_id -> DocumentChunk
        self.id_to_chunk: Dict[int, DocumentChunk] = {}
        # Tracking: chunk_id -> int_id
        self.chunk_id_to_int: Dict[str, int] = {}
        # Tracking: doc_id -> int_ids, and doc_id -> version / replication info
        self.doc_to_ids: Dict[str, Set[int]] = {}
        self.docs: Dict[str, DocumentInfo] = {}
        self._next_id: int = 1

    def _check_vectors(self, embeddings: np.ndarray) -> np.ndarray:
        vecs = np.asarray(embeddings, dtype=np.float32)
        if vecs.ndim == 1:
            vecs = np.expand_dims(vecs, axis=0)
        if vecs.ndim != 2 or vecs.shape[1] != self.dim:
            raise ValueError(f"Expected vectors of dimension {self.dim}, got shape {vecs.shape}")
        return vecs

    def add_chunks(self, chunks: List[DocumentChunk], embeddings: np.ndarray) -> List[int]:
        """Appends document chunks and embedding vectors to the index (no replacement)."""
        if not chunks:
            return []
        vecs = self._check_vectors(embeddings)
        if len(chunks) != len(vecs):
            raise ValueError(f"Mismatch between chunks ({len(chunks)}) and embeddings ({len(vecs)})")

        with self._lock:
            assigned_ids = []
            for chunk in chunks:
                # Re-adding a known chunk_id replaces the old vector instead of duplicating it
                old_id = self.chunk_id_to_int.get(chunk.chunk_id)
                if old_id is not None:
                    self._remove_ids([old_id])

                int_id = self._next_id
                self._next_id += 1
                assigned_ids.append(int_id)
                self.id_to_chunk[int_id] = chunk
                self.chunk_id_to_int[chunk.chunk_id] = int_id
                self.doc_to_ids.setdefault(chunk.doc_id, set()).add(int_id)
                info = self.docs.setdefault(chunk.doc_id, DocumentInfo(doc_id=chunk.doc_id))
                info.chunk_count = len(self.doc_to_ids[chunk.doc_id])

            self.index.add_with_ids(vecs, np.array(assigned_ids, dtype=np.int64))
            logger.info(f"[{self.shard_id}] Added {len(chunks)} vectors to FAISS index. Total count: {self.index.ntotal}")
            return assigned_ids

    def upsert_document(
        self,
        doc_id: str,
        chunks: List[DocumentChunk],
        embeddings: np.ndarray,
        version: int = 0,
        replication_factor: int = 0,
    ) -> bool:
        """Atomically replaces every chunk of `doc_id`. Returns False (and changes nothing) if `version` is older than the stored one."""
        vecs = self._check_vectors(embeddings) if chunks else np.empty((0, self.dim), dtype=np.float32)
        if len(chunks) != len(vecs):
            raise ValueError(f"Mismatch between chunks ({len(chunks)}) and embeddings ({len(vecs)})")
        if any(c.doc_id != doc_id for c in chunks):
            raise ValueError(f"All chunks must belong to doc_id '{doc_id}'")

        with self._lock:
            current = self.docs.get(doc_id)
            if current is not None and version and current.version > version:
                logger.info(f"[{self.shard_id}] Ignoring stale write for {doc_id} (v{version} < v{current.version})")
                return False
            self.delete_by_doc_id(doc_id)
            if chunks:
                self.add_chunks(chunks, vecs)
            info = self.docs.setdefault(doc_id, DocumentInfo(doc_id=doc_id))
            info.version = version
            info.replication_factor = replication_factor
            info.chunk_count = len(chunks)
            info.metadata = {k: v for k, v in (chunks[0].metadata if chunks else {}).items() if k != "chunk_index"}
            return True

    def search(self, query_vector: np.ndarray, top_k: int = 5) -> List[SearchResult]:
        """Searches FAISS index for top-k most similar chunks by cosine similarity."""
        query_vector = self._check_vectors(query_vector)
        with self._lock:
            if self.index.ntotal == 0 or top_k <= 0:
                return []

            k = min(top_k, self.index.ntotal)
            scores, ids = self.index.search(query_vector[:1], k)

            results = []
            for score, int_id in zip(scores[0], ids[0]):
                if int_id == -1 or int_id not in self.id_to_chunk:
                    continue
                chunk = self.id_to_chunk[int_id]
                info = self.docs.get(chunk.doc_id)
                results.append(SearchResult(
                    chunk_id=chunk.chunk_id,
                    doc_id=chunk.doc_id,
                    text=chunk.text,
                    score=float(score),
                    chunk_index=chunk.chunk_index,
                    metadata=chunk.metadata,
                    shard_id=self.shard_id,
                    doc_version=info.version if info else 0,
                ))
            return results

    def _remove_ids(self, int_ids: List[int]):
        if not int_ids:
            return
        self.index.remove_ids(faiss.IDSelectorBatch(np.array(int_ids, dtype=np.int64)))
        for int_id in int_ids:
            chunk = self.id_to_chunk.pop(int_id, None)
            if chunk is None:
                continue
            self.chunk_id_to_int.pop(chunk.chunk_id, None)
            doc_ids = self.doc_to_ids.get(chunk.doc_id)
            if doc_ids is not None:
                doc_ids.discard(int_id)

    def delete_by_doc_id(self, doc_id: str, below_version: int = 0) -> int:
        """Removes all chunks of `doc_id`. With `below_version`, only if the stored version is older than it."""
        with self._lock:
            info = self.docs.get(doc_id)
            if info is None:
                return 0
            if below_version and info.version >= below_version:
                return 0
            target_ids = list(self.doc_to_ids.pop(doc_id, set()))
            self._remove_ids(target_ids)
            self.docs.pop(doc_id, None)
            if target_ids:
                logger.info(f"[{self.shard_id}] Removed {len(target_ids)} vectors for doc_id {doc_id}")
            return len(target_ids)

    def list_documents(self) -> List[DocumentInfo]:
        """Returns a snapshot of every document held by this store."""
        with self._lock:
            return [info.model_copy() for info in self.docs.values()]

    def get_document(self, doc_id: str) -> Tuple[Optional[DocumentInfo], List[DocumentChunk], np.ndarray]:
        """Returns (info, chunks ordered by chunk_index, their embeddings) so the document can be copied to another shard."""
        with self._lock:
            info = self.docs.get(doc_id)
            int_ids = sorted(self.doc_to_ids.get(doc_id, set()), key=lambda i: self.id_to_chunk[i].chunk_index)
            if info is None or not int_ids:
                return info, [], np.empty((0, self.dim), dtype=np.float32)
            vecs = np.vstack([self.index.reconstruct(int(i)) for i in int_ids]).astype(np.float32)
            return info.model_copy(), [self.id_to_chunk[i] for i in int_ids], vecs

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
            self.doc_to_ids.clear()
            self.docs.clear()
            self._next_id = 1

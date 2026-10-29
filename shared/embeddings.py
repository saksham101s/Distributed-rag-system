import logging
import threading
from typing import List
import numpy as np

from shared.config import config

logger = logging.getLogger("EmbeddingsEngine")


class EmbeddingsEngine:
    """Wraps a local SentenceTransformers embedding model (default all-MiniLM-L6-v2)."""

    def __init__(self, model_name: str = config.EMBEDDING_MODEL_NAME, dim: int = config.EMBEDDING_DIM):
        self.model_name = model_name
        self.dim = dim
        self._model = None
        self._load_lock = threading.Lock()

    @property
    def model(self):
        """Lazy, thread-safe loader for the SentenceTransformer model."""
        if self._model is None:
            with self._load_lock:
                if self._model is None:
                    from sentence_transformers import SentenceTransformer
                    logger.info(f"Loading local embedding model: {self.model_name}")
                    self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed_text(self, text: str) -> np.ndarray:
        """Embeds a single string into a normalized float32 vector."""
        vec = self.model.encode([text], convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)[0]
        return vec.astype(np.float32)

    def embed_batch(self, texts: List[str]) -> np.ndarray:
        """Embeds a list of strings into a normalized float32 matrix (N x dim)."""
        if not texts:
            return np.empty((0, self.dim), dtype=np.float32)
        vecs = self.model.encode(texts, convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)
        return vecs.astype(np.float32)


# Global singleton instance
embeddings_engine = EmbeddingsEngine()

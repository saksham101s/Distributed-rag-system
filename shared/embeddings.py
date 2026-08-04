import logging
from typing import List, Union
import numpy as np

logger = logging.getLogger("EmbeddingsEngine")

class EmbeddingsEngine:
    """Wraps SentenceTransformers local embedding model (all-MiniLM-L6-v2)."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None

    @property
    def model(self):
        """Lazy loader for SentenceTransformer model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                logger.info(f"Loading local embedding model: {self.model_name}")
                self._model = SentenceTransformer(self.model_name)
            except Exception as e:
                logger.error(f"Failed to load SentenceTransformer model {self.model_name}: {e}")
                raise e
        return self._model

    def embed_text(self, text: str) -> np.ndarray:
        """Embeds a single string into a 384-dimensional normalized float32 vector."""
        vec = self.model.encode([text], convert_to_numpy=True, normalize_embeddings=True)[0]
        return vec.astype(np.float32)

    def embed_batch(self, texts: List[str]) -> np.ndarray:
        """Embeds a list of strings into normalized float32 vectors matrix (N x 384)."""
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
        vecs = self.model.encode(texts, convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)
        return vecs.astype(np.float32)


# Global singleton instance
embeddings_engine = EmbeddingsEngine()

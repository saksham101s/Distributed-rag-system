import hashlib
import json
import logging
import time
from typing import Optional, Dict, Any, Tuple
from shared.config import config

logger = logging.getLogger("QueryCache")

GENERATION_KEY = "rag:cache:generation"


class QueryCache:
    """Redis-backed query result cache with TTL.

    Invalidation is O(1): every key embeds a generation number, and `invalidate_all()` just bumps it.
    Entries from older generations are never read again and expire through their TTL.
    """

    def __init__(self, ttl_seconds: int = config.CACHE_TTL_SECONDS):
        self.ttl_seconds = ttl_seconds
        self.redis_client = None
        # In-process fallback: key -> (expires_at, payload)
        self._memory_fallback: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._memory_generation = 0
        self._init_redis()

    def _init_redis(self):
        """Attempts to initialize Redis client; falls back to in-memory dict if Redis unavailable."""
        try:
            import redis
            client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=0,
                socket_timeout=1.0,
                decode_responses=True
            )
            client.ping()
            self.redis_client = client
            logger.info(f"QueryCache: Connected to Redis at {config.REDIS_HOST}:{config.REDIS_PORT}")
        except Exception as e:
            logger.warning(f"QueryCache: Redis unavailable ({e}). Using in-memory fallback cache.")
            self.redis_client = None

    def _generation(self) -> int:
        if self.redis_client:
            return int(self.redis_client.get(GENERATION_KEY) or 0)
        return self._memory_generation

    def _get_key(self, query: str, top_k: int, generation: int) -> str:
        """Generates deterministic cache key from query text, top_k and cache generation."""
        h = hashlib.md5(f"{query.strip().lower()}_top{top_k}".encode()).hexdigest()
        return f"rag:cache:g{generation}:{h}"

    def get(self, query: str, top_k: int = 5) -> Optional[Dict[str, Any]]:
        """Retrieves cached RAG query result if available."""
        try:
            key = self._get_key(query, top_k, self._generation())
            if self.redis_client:
                data = self.redis_client.get(key)
                payload = json.loads(data) if data else None
            else:
                entry = self._memory_fallback.get(key)
                payload = None
                if entry:
                    expires_at, payload = entry
                    if expires_at < time.time():
                        self._memory_fallback.pop(key, None)
                        payload = None
        except Exception as e:
            logger.error(f"QueryCache GET error: {e}")
            return None

        logger.info(f"QueryCache: {'HIT' if payload else 'MISS'} for query '{query[:30]}...'")
        return payload

    def set(self, query: str, top_k: int, response_data: Dict[str, Any]):
        """Stores RAG query result in cache with TTL."""
        try:
            key = self._get_key(query, top_k, self._generation())
            if self.redis_client:
                self.redis_client.setex(key, self.ttl_seconds, json.dumps(response_data))
            else:
                self._memory_fallback[key] = (time.time() + self.ttl_seconds, response_data)
        except Exception as e:
            logger.error(f"QueryCache SET error: {e}")

    def invalidate_all(self):
        """Invalidates every cached answer (called when the document set changes)."""
        try:
            if self.redis_client:
                generation = self.redis_client.incr(GENERATION_KEY)
            else:
                self._memory_generation += 1
                self._memory_fallback.clear()
                generation = self._memory_generation
            logger.info(f"QueryCache: invalidated (now generation {generation}).")
        except Exception as e:
            logger.error(f"QueryCache invalidation error: {e}")

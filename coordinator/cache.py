import hashlib
import json
import logging
from typing import Optional, Dict, Any
from shared.config import config

logger = logging.getLogger("QueryCache")


class QueryCache:
    """Redis-backed query result cache with TTL and explicit invalidation on ingestion."""

    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self.redis_client = None
        self._memory_fallback: Dict[str, Dict[str, Any]] = {}
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

    def _get_key(self, query: str, top_k: int) -> str:
        """Generates deterministic cache key from query text and top_k."""
        h = hashlib.md5(f"{query.strip().lower()}_top{top_k}".encode()).hexdigest()
        return f"rag:cache:{h}"

    def get(self, query: str, top_k: int = 5) -> Optional[Dict[str, Any]]:
        """Retrieves cached RAG query result if available."""
        key = self._get_key(query, top_k)
        
        if self.redis_client:
            try:
                data = self.redis_client.get(key)
                if data:
                    logger.info(f"QueryCache: HIT for query '{query[:30]}...'")
                    return json.loads(data)
            except Exception as e:
                logger.error(f"QueryCache Redis GET error: {e}")
        else:
            if key in self._memory_fallback:
                logger.info(f"QueryCache (Memory): HIT for query '{query[:30]}...'")
                return self._memory_fallback[key]
                
        logger.info(f"QueryCache: MISS for query '{query[:30]}...'")
        return None

    def set(self, query: str, top_k: int, response_data: Dict[str, Any]):
        """Stores RAG query result in cache with TTL."""
        key = self._get_key(query, top_k)
        
        if self.redis_client:
            try:
                self.redis_client.setex(key, self.ttl_seconds, json.dumps(response_data))
            except Exception as e:
                logger.error(f"QueryCache Redis SET error: {e}")
        else:
            self._memory_fallback[key] = response_data

    def invalidate_all(self):
        """Invalidates cache upon document ingestion."""
        if self.redis_client:
            try:
                keys = self.redis_client.keys("rag:cache:*")
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"QueryCache: Invalidated {len(keys)} cached entries due to document ingestion.")
            except Exception as e:
                logger.error(f"QueryCache Redis invalidation error: {e}")
        else:
            self._memory_fallback.clear()
            logger.info("QueryCache (Memory): Cleared all cached entries due to document ingestion.")

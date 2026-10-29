import os
from typing import List, Tuple
from pydantic import BaseModel


class SystemConfig(BaseModel):
    """Global configuration settings for the Distributed RAG system."""

    # Embedding Configuration
    EMBEDDING_MODEL_NAME: str = os.getenv("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
    EMBEDDING_DIM: int = int(os.getenv("EMBEDDING_DIM", "384"))

    # Document Chunking Configuration
    DEFAULT_CHUNK_SIZE: int = int(os.getenv("DEFAULT_CHUNK_SIZE", "500"))
    DEFAULT_CHUNK_OVERLAP: int = int(os.getenv("DEFAULT_CHUNK_OVERLAP", "50"))

    # Vector Search Configuration
    DEFAULT_TOP_K: int = int(os.getenv("DEFAULT_TOP_K", "5"))
    SCORE_THRESHOLD: float = float(os.getenv("SCORE_THRESHOLD", "0.0"))

    # Distributed Architecture Configuration
    # Comma-separated "shard_id=host:port" list of shard nodes the coordinator manages
    SHARD_NODES: str = os.getenv(
        "SHARD_NODES", "shard_0=localhost:50051,shard_1=localhost:50052,shard_2=localhost:50053"
    )
    DEFAULT_REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", "2"))
    VIRTUAL_NODES_PER_SHARD: int = int(os.getenv("VIRTUAL_NODES", "100"))
    SHARD_TIMEOUT_SECONDS: float = float(os.getenv("SHARD_TIMEOUT_SECONDS", "1.5"))
    HEARTBEAT_INTERVAL_SECONDS: float = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2.0"))
    # Periodic anti-entropy pass; membership changes also trigger one immediately. 0 disables the periodic pass.
    REPAIR_INTERVAL_SECONDS: float = float(os.getenv("REPAIR_INTERVAL_SECONDS", "30"))

    # LLM Provider Configuration ("mock", "gemini", "claude")
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "mock")
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-opus-5-5")
    CLAUDE_EFFORT: str = os.getenv("CLAUDE_EFFORT", "medium")

    # Infrastructure Endpoints
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "300"))

    # Tracing exporter: "none" (spans created but dropped) or "console"
    TRACING_EXPORTER: str = os.getenv("TRACING_EXPORTER", "none")

    # File upload ingest
    MAX_UPLOAD_MB: float = float(os.getenv("MAX_UPLOAD_MB", "10"))

    # Dev-only fault injection endpoints (coordinator) and SetFault RPC (shards). Never enable in production.
    CHAOS_ENABLED: bool = os.getenv("CHAOS_ENABLED", "false").lower() in ("1", "true", "yes")

    # Optional shared API key. When set, every /api/v1 route needs `Authorization: Bearer <key>` or `X-API-Key: <key>`
    # (or `?api_key=` on GET /cluster/events, since EventSource can't send headers). Empty means no auth (dev default).
    API_KEY: str = os.getenv("API_KEY", "")

    def shard_nodes(self) -> List[Tuple[str, str, int]]:
        """Parses SHARD_NODES into (shard_id, host, port) tuples."""
        nodes = []
        for entry in filter(None, (e.strip() for e in self.SHARD_NODES.split(","))):
            shard_id, _, address = entry.partition("=")
            host, _, port = address.rpartition(":")
            if not shard_id or not host or not port.isdigit():
                raise ValueError(f"Invalid SHARD_NODES entry '{entry}', expected shard_id=host:port")
            nodes.append((shard_id.strip(), host.strip(), int(port)))
        return nodes


config = SystemConfig()

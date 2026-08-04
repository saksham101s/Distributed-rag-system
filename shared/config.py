import os
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
    DEFAULT_REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", "2"))
    VIRTUAL_NODES_PER_SHARD: int = int(os.getenv("VIRTUAL_NODES", "100"))
    SHARD_TIMEOUT_SECONDS: float = float(os.getenv("SHARD_TIMEOUT_SECONDS", "1.5"))
    
    # LLM Provider Configuration ("mock", "gemini", "claude")
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "mock")
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    
    # Infrastructure Endpoints
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    POSTGRES_URI: str = os.getenv("POSTGRES_URI", "postgresql://rag_user:rag_pass@localhost:5432/rag_db")

config = SystemConfig()

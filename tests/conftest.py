import os
import sys

# Make `shared`, `coordinator`, `proto_gen` importable however pytest is launched
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Keep tests hermetic: no Redis, no paid LLM calls
os.environ.setdefault("REDIS_HOST", "127.0.0.1")
os.environ.setdefault("REDIS_PORT", "1")
os.environ.setdefault("LLM_PROVIDER", "mock")

# CLAUDE.md

Distributed RAG system: a FastAPI coordinator, 3 gRPC FAISS shard nodes, a consistent hash ring, Redis cache, and a pluggable LLM.

## Start every session here (don't re-read the whole codebase)
1. `docs/MEMORY.md`: current status, the user's plan, decisions, session log.
2. `docs/ARCHITECTURE.md`: file map, APIs, data flows, config. Open source files only for the parts you are changing.
3. `docs/KNOWN_ISSUES.md`: known bugs and gaps, numbered.

## Keep the docs current (every session that changes something)
- Code changed, so a flow, API, proto, config or file layout changed: update `docs/ARCHITECTURE.md` and bump its "Last verified" line.
- Fixed or found an issue: update `docs/KNOWN_ISSUES.md`.
- Always: append to the session log in `docs/MEMORY.md`, and record any new decisions or plan progress there.

## Conventions
- Run everything from the repo root: `python -m pytest tests/unit tests/integration -v`.
- gRPC code is imported from `proto_gen/` (not `proto/`). After regenerating, fix the import in `vector_service_pb2_grpc.py` to `from proto_gen import ...`.
- Services add the repo root to `sys.path` themselves, so they can be run as `python coordinator/coordinator_service.py`.
- Code style: type-hinted, pydantic models, module-level loggers named in CamelCase, short docstrings.

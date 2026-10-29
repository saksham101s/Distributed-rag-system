# Project Memory

> Running notebook shared by the user and Claude. **Read this first**, then `ARCHITECTURE.md`.
> Update at the end of every working session: status, decisions, plan progress, and the session log.

## Current status
- Working prototype with real replication: 3 gRPC shards, FastAPI coordinator, Redis cache, FAISS, mock/Gemini/Claude LLM.
- 2026-10-29: 24 of the original 26 known issues are fixed (#5 is only partly fixed, and #12 waits on the UI rework). Upserts, deletes, an anti-entropy repair loop, query dedup, tracing and Docker targets were added.
- Tests: 19 pytest tests pass (`python -m pytest`), plus the chaos script (`python tests/chaos/chaos_test.py`). An end-to-end HTTP smoke test (3 shard processes + uvicorn) also passed.
- Git: branch `main`, last commit `621471e`. **All 2026-10-29 work is uncommitted** (the user hasn't asked for a commit).
  The parent folder `~/Documents/Distributed-rag-system/` is not a repo; the repo is the inner `Distributed-rag-system/`.
- No system pip/venv on this machine (`python3` lacks ensurepip). Claude tested with a uv-made venv in its session scratchpad.
  The user needs their own environment: e.g. `uv venv && uv pip install -r requirements.txt`.

## User's plan
- Goal (2026-10-29): "Fix the issues and also plan the next steps to improve the project, after that we will update the UI."
- Order: (1) fix known issues ✅ → (2) agree on the backend roadmap below → (3) UI rework.
- Approach / constraints: _none given yet_.

## Roadmap (proposed 2026-10-29, not yet approved by the user)
**Phase A: backend features the new UI needs** (do before or alongside the UI)
1. `GET /api/v1/documents`: list docs with version, chunk count and which shards hold them (from `ListDocuments`).
2. Streaming answers (SSE) for `POST /query`, so the chat UI can render tokens as they arrive.
3. Cluster event stream (SSE): node up/down, repair copied/trimmed, ingest. Replaces 2s polling.
4. File upload ingest (txt/md/pdf) with server-side text extraction.
5. Dev-only chaos endpoints (pause/kill a shard) so the UI can demo failover live.

**Phase B: robustness** (open issues 5, 27, 29)
6. Shard snapshots: `faiss.write_index` + doc metadata to a volume, on an interval and at shutdown; load at startup.
7. Keep tombstones and node registry in Redis so a coordinator restart doesn't lose them.
8. Configurable write quorum `W` (default 1). Return 502/`partial` when fewer than W shards ack.

**Phase C: retrieval quality**
9. Deterministic chunk ids (`doc_id#chunk_index`) and sentence/token-aware chunking.
10. Hybrid retrieval (BM25 + vectors) and an optional cross-encoder re-ranker. Add a small eval set (recall@k) to measure it.
11. Optional ANN index per shard (HNSW / IVF) behind a config flag (issue 32).
12. Citations: number sources in the prompt, return the cited source ids with the answer.

**Phase D: ops**
13. OTLP exporter plus Jaeger and Grafana in docker-compose, with a Grafana dashboard for the existing metrics (issue 36).
14. CI: GitHub Actions running pytest + the chaos script.
15. API key auth, CORS allow-list, request size limits (issue 34).

**Phase E: scale-out (stretch)**
16. Multiple coordinators sharing state (Redis/etcd), and leader-only repair.
17. Merkle/digest-based repair instead of listing every doc (issue 30); hedged reads (issue 31).

**UI rework (after the roadmap is agreed)**: replace the single HTML file with a Vite + React + TypeScript app.
Views: chat with streamed answers and source cards (shard badges, scores), document manager (upload, list, delete, placement),
live cluster topology (ring view, node health, vector counts, repair activity), and per-query telemetry (per-shard latency, degraded flag).

## Decisions log
| Date | Decision | Why |
|---|---|---|
| 2026-10-25 | Docs live in `docs/` (ARCHITECTURE, MEMORY, KNOWN_ISSUES), indexed from `CLAUDE.md` | So future sessions read these instead of re-reading all the code |
| 2026-10-29 | Reads stay broadcast; placement converges via a copy-then-trim repair loop | Semantic queries aren't keyed by doc_id, so ring-routed reads don't apply; repair makes placement match the ring anyway |
| 2026-10-29 | Last-writer-wins by coordinator `time_ns()` `doc_version`; shards reject older versions | Single coordinator ⇒ one clock; this makes upserts and repair safe against races |
| 2026-10-29 | Write succeeds with ≥1 ack (`partial`), repair fills the rest | Keeps the AP bias; a configurable quorum is on the roadmap |
| 2026-10-29 | Removed README claims for FASTEST_REPLICA/READ_QUORUM, Redis Streams and Postgres instead of building them | They don't fit the design (or weren't needed); the README should only claim what exists |
| 2026-10-29 | Claude provider defaults to `claude-opus-5-5`, effort `medium`, server-side refusal fallback on | Current default model; configurable via `CLAUDE_MODEL` / `CLAUDE_EFFORT` |
| 2026-10-29 | Proto regenerated with grpcio-tools 1.85 ⇒ requires grpcio ≥ 1.85, protobuf ≥ 7.35.1 | Generated code enforces these minimums at import |

## Session log
- **2026-10-25**: Read the whole codebase. Created `CLAUDE.md`, `docs/ARCHITECTURE.md`, `docs/MEMORY.md` and `docs/KNOWN_ISSUES.md` (26 issues). Waiting for the user's plan.
- **2026-10-29**: User asked to fix the issues and plan the next steps, with the UI after. Fixed 1-4, 6-11 and 13-26, and partly fixed 5 (re-replication done, no disk persistence). Added 3 RPCs (Delete/List/Fetch), doc versions, upsert, `DELETE /documents/{id}`, `POST /cluster/repair`, the repair loop with tombstones, `merge_results` dedup, generation-based cache invalidation, OTel instrumentation, multi-target Dockerfile, `requirements-shard.txt`, `pytest.ini`/`conftest.py`, and new tests (`test_faiss_store.py`, `test_repair.py`). Rewrote the chaos test, fixed README claims, and deleted `proto/` and the tracked pycache. Logged new issues 27-36. Wrote the roadmap above. Nothing committed.

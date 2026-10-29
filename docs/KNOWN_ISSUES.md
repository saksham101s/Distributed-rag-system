# Known Issues & Gaps

> Severity: 🔴 broken / wrong results · 🟠 claim in README not backed by code · 🟡 quality / perf.
> The original 26 issues were found on 2026-10-25 and worked through on 2026-10-29 (see "Resolved" at the bottom).
> Numbers are stable: new issues get the next number, resolved ones move down.

## Open

### 🔴 Correctness / durability
5. **No durability**: shard indexes are in process memory only. Repair refills a restarted shard from surviving
   replicas, but if all N replicas of a doc go down, or the whole cluster restarts, that doc is gone. *(Partially fixed: re-replication done, persistence not.)*
27. **Tombstones live in coordinator memory**: if the coordinator restarts while a shard that missed a delete is down,
   repair will copy the deleted doc back when that shard returns.
28. **Coordinator is a single point of failure**: ring, node health, tombstones and repair all live in one process.
29. **No write quorum**: an ingest succeeds with 1 ack (`status: "partial"`). Until repair runs, that doc has a single copy.

### 🟡 Quality / performance
30. **Repair lists every document on every pass**: O(total docs) per pass and per membership change. Fine for the demo; needs
   per-shard digests (Merkle tree / hash of doc versions per ring range) at scale.
31. **Reads broadcast to every healthy shard**: cost grows with shard count. No hedged requests or per-shard top-k tuning.
32. **Exact search only** (`IndexFlatIP`): O(N) per query per shard. No IVF/HNSW option.
34. **Single shared API key only** (optional `API_KEY`, 2026-10-29): no users/roles, no rate limits, CORS `*`, and no request size limits on JSON ingest. The UI keeps the key in localStorage, and the event stream sends it in the query string (it can show up in access logs).
35. `coordinator/baseline_rag.py` also binds port 8000, so it can't run next to the coordinator.
36. No OTLP exporter or tracing backend (Jaeger/Tempo) in docker-compose; spans only go to the console.

37. **The document catalog lists every shard on every request**: `GET /documents` and `GET /documents/{id}` call
   `ListDocuments` on all healthy shards each time, so they're O(total docs). Pagination happens after the fan-out.
38. **Chaos `down` is simulated at the RPC layer**: the shard process and TCP connection stay up. A real crash is covered by the chaos script.
39. **Upload limits**: no OCR for scanned PDFs (they return 422), the whole file is read into memory (capped by `MAX_UPLOAD_MB`), and there's no `.docx`/HTML support.
40. **Event history is in coordinator memory** (last 50 events) and is lost when the coordinator restarts (see 28).
45. **No browser end-to-end tests in the repo or CI**: the Playwright flows Claude ran (sign in, ask, reload, mobile panel, chaos) live in its scratchpad only, and there is no CI workflow at all.

## Resolved (2026-10-29)
1. Duplicate results: `ScatterGatherEngine.merge_results` dedups by `chunk_id` and drops stale doc versions.
2. docker-compose shard addresses: `SHARD_NODES` env var; compose uses service names.
3. Ingest always reported success: `resp.success` checked, per-shard timeouts, `failed_shards` + `partial`/502.
4. Re-ingest duplicated chunks: shard-side per-doc upsert with versions; older copies on non-target shards are deleted; `DELETE /documents/{id}` added.
6. Ring changes didn't move data: repair loop copies to new ring targets and trims old ones.
7. `FASTEST_REPLICA`/`READ_QUORUM`: claim removed from README (semantic queries can't be ring-routed; see README CAP section).
8. Tracing: FastAPI + grpc.aio client/server instrumentors and manual spans; exporter selectable.
9. `rag_shard_request_duration_seconds` is now observed per shard search.
10. Chaos test now SIGKILLs a shard during a 50-query burst using the full stub map, then checks repair restores placement.
11. Redis Streams / Postgres claims removed; `POSTGRES_URI` removed.
13. Embedding, LLM and Redis calls run in `asyncio.to_thread`.
14. Cache invalidation is O(1) (generation counter), no `KEYS`.
15. Ring keeps a cached hash list; `get_nodes` no longer rebuilds it. `ring_distribution` now reports true key-space share.
16. Heartbeats run concurrently and update state under the lock; reconnect backoff capped at 2s.
17. All config values are used (`EMBEDDING_MODEL_NAME`, `DEFAULT_TOP_K`, `SCORE_THRESHOLD`, `REPLICATION_FACTOR`).
18. Claude model → `claude-opus-5-5` (configurable via `CLAUDE_MODEL`), refusal fallback enabled; `anthropic` added to requirements.
19. `on_event("startup")` replaced with a lifespan handler (also closes channels).
20. Stale `proto/` deleted.
21. `__pycache__` untracked; `.gitignore` and `.dockerignore` added.
22. `generate_pdf_guide.py` defaults to `docs/ARCHITECTURE.md` and takes input/output paths.
23. Multi-target Dockerfile: slim shard image (`requirements-shard.txt`), coordinator image with CPU torch and the model baked in.
24. Frontend CDN scripts pinned (React 18.3.1 production builds, Babel 7.26.10). Superseded by the Vite build (no CDN scripts now).
25. Shards validate embedding dimension; search errors return gRPC `INVALID_ARGUMENT`/`INTERNAL` instead of empty results.
26. `pytest.ini` + `tests/conftest.py` added.
12. Frontend rebuilt as a Vite + React + TS app driven by the SSE API (2026-10-29, branch `ui-rework`); the old single HTML file is removed.
33. Source attribution: the merge now returns `replica_shards`, and the UI shows "also on …" (2026-10-29).
41. UI auth: optional `API_KEY` on the coordinator (bearer / `X-API-Key`, `?api_key=` for the event stream), and a sign-in screen and Sign out in the UI (2026-10-29). Remaining limits are in 34.
42. UI component tests: vitest + jsdom + Testing Library for `ChatView`, `AnswerParts` and `App` (16 frontend tests total). A browser end-to-end suite (Playwright) is still not in the repo; see 45.
43. Below 960px the Ask side panel is a slide-over opened by a **Shards** button (2026-10-29).
44. Chat history is saved in localStorage (last 30 turns), restored on reload, with **Clear conversation** (2026-10-29).

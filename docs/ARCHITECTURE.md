# Architecture — Distributed RAG System

> Source of truth for *how the code works today*. Keep it in sync with the code.
> Last verified against code: 2026-10-29 (branch `ui-rework`, on top of `fc9b117`; includes the API key, saved chat, narrow-screen panel and component tests).

## 1. What it is

A sharded, replicated vector-search + RAG backend in Python:

- **Coordinator** (FastAPI, port 8000): chunks and embeds documents, routes them to shards via a
  consistent hash ring, fans queries out to all healthy shards over gRPC, merges and de-duplicates results, calls an LLM,
  caches answers in Redis, and runs a heartbeat loop plus an anti-entropy **repair loop**. Streams answers and
  cluster events to the UI over Server-Sent Events; ingests uploaded files; exposes dev-only fault injection.
- **Shard nodes** (gRPC aio, ports 50051-50053): each holds an **in-memory** FAISS index plus per-document
  version info. Plain storage: no embedding, no hashing, no knowledge of other shards.
- **Redis**: query-result cache (falls back to an in-process dict with TTL if Redis is unreachable).
- **Prometheus**: scrapes the coordinator's `/metrics`.
- **Frontend**: Vite + React 18 + TypeScript app in `frontend/`. In dev it runs on :5173 and proxies `/api` to the coordinator; in production the coordinator serves `frontend/dist` at `/`. It uses only the REST + SSE API (no polling).

```
Browser (frontend/, served from frontend/dist)
   │ HTTP JSON
   ▼
Coordinator (coordinator/coordinator_service.py)
   ├─ ConsistentHashRing (shared/consistent_hash.py)   ← write routing + repair placement
   ├─ DocumentProcessor (shared/document_processor.py) ← chunking
   ├─ EmbeddingsEngine (shared/embeddings.py)          ← MiniLM-L6-v2, 384-d, normalized (run via asyncio.to_thread)
   ├─ ScatterGatherEngine (coordinator/scatter_gather.py) ← fan-out, dedup, stale-version filter
   ├─ QueryCache (coordinator/cache.py) ── Redis       ← generation-number invalidation
   ├─ LLMGenerator (shared/llm.py)  mock | gemini | claude (run via asyncio.to_thread)
   ├─ heartbeat loop + repair loop (CoordinatorState)
   ├─ EventBus (coordinator/events.py)                 ← SSE fan-out of cluster events
   ├─ text extraction (shared/text_extraction.py)      ← .txt/.md/.pdf uploads
   └─ Prometheus /metrics + OpenTelemetry (shared/telemetry.py)
   │ gRPC (proto_gen/vector_service.proto)
   ▼
Shard nodes ×3 (shard_node/shard_service.py) → FAISSVectorStore (shared/faiss_store.py)
```

## 2. File map

| Path | Role |
|---|---|
| `coordinator/coordinator_service.py` | FastAPI app (lifespan handler), `CoordinatorState` (nodes, ring, cache, tombstones, faults, `events`; `ingest`, `delete`, `repair`, `check_node_health`, `status_snapshot`, `list_documents`, `get_document`, `set_fault`), heartbeat + repair loops, REST + SSE endpoints. Entry point. |
| `coordinator/events.py` | `EventBus` (bounded per-subscriber queues, drop-oldest; last 50 non-status events kept as `history`), `ClusterEvent`, `sse()` frame formatter. |
| `coordinator/scatter_gather.py` | `ScatterGatherEngine.execute_scatter_gather()`: parallel `SearchVector`, per-shard timeout, observes `rag_shard_request_duration_seconds`. `merge_results()` (static): drop stale doc versions → dedup by `chunk_id` → score threshold → sort → top_k. |
| `coordinator/cache.py` | `QueryCache`: key `rag:cache:g{generation}:md5(lower(query)_top{k})`, TTL `CACHE_TTL_SECONDS`. `invalidate_all()` = `INCR rag:cache:generation` (O(1)). |
| `coordinator/baseline_rag.py` | Standalone single-node RAG FastAPI app (no shards/gRPC). Reference/benchmark baseline. Also port 8000. |
| `shard_node/shard_service.py` | `VectorStoreServicer`: IngestVector (per-doc upsert, version-checked), SearchVector, HealthCheck, DeleteDocument, ListDocuments, FetchDocument, SetFault. Every RPC except SetFault passes `_fault_gate` (down → `UNAVAILABLE`, slow → sleep). Validates embedding dimension; search errors return gRPC status codes. CLI: `--shard-id`, `--port`. |
| `shared/config.py` | `SystemConfig` read from env vars; `config.shard_nodes()` parses `SHARD_NODES`. Singleton `config`. |
| `shared/consistent_hash.py` | `ConsistentHashRing`: MD5 → first 32 bits, vnodes `"{node}#vnode_{i}"`, sorted list + cached hash list for bisect, clockwise walk for distinct replicas. `ring_distribution()` = % of the key space owned (arc length). |
| `shared/document_processor.py` | `DocumentChunk` model (uuid `chunk_id`), `DocumentProcessor` char-based chunking (default 500/50) snapped to spaces. |
| `shared/embeddings.py` | `EmbeddingsEngine` lazy, thread-safe model load (`EMBEDDING_MODEL_NAME`); `embed_text`, `embed_batch`. Singleton `embeddings_engine`. |
| `shared/faiss_store.py` | `FAISSVectorStore`: `IndexIDMap2(IndexFlatIP)`, int-id ↔ chunk, `doc_to_ids`, `docs: doc_id → DocumentInfo(version, replication_factor, chunk_count)`. `upsert_document`, `add_chunks`, `search`, `delete_by_doc_id(below_version)`, `list_documents`, `get_document` (reconstructs embeddings), `count`, `clear`. Raises `ValueError` on dimension mismatch. |
| `shared/text_extraction.py` | `extract_text(filename, data, content_type)` for .txt/.md/.rst/.csv/.log (UTF-8/UTF-16) and .pdf (pypdf), whitespace-normalized; `UnsupportedFileType`; `doc_id_from_filename()` slug. |
| `shared/llm.py` | `LLMGenerator`: `generate_answer()` and `stream_answer()` (blocking generator of `StreamEvent` token/reset/done; provider failure mid-stream → `reset` + mock). Provider from `LLM_PROVIDER` + key present, else mock. Claude: `CLAUDE_MODEL` (default `claude-opus-5-5`), `CLAUDE_EFFORT`, server-side refusal fallback (`fallbacks="default"`, beta `server-side-fallback-2026-07-01`). Gemini: `GEMINI_MODEL`. Any API error or refusal → mock. |
| `shared/telemetry.py` | Prometheus metrics (§6), `setup_telemetry(exporter)`, `instrument_fastapi`, `instrument_grpc_client`, `instrument_grpc_server`, `span()` context manager. |
| `proto_gen/` | Generated gRPC code + `.proto` (generated with grpcio-tools 1.85 → needs grpcio ≥ 1.85, protobuf ≥ 7.35.1). |
| `tests/conftest.py`, `pytest.ini` | Adds repo root to `sys.path`, `asyncio_mode=auto`, forces mock LLM and an unreachable Redis (memory cache). |
| `tests/unit/` | `test_consistent_hash.py`, `test_baseline.py` (chunking, embeddings+FAISS, mock LLM), `test_faiss_store.py` (upsert, stale writes, delete-below-version, export, dim checks, merge dedup). |
| `tests/integration/test_scatter_gather.py` | In-process gRPC servers; ingest/search and partial failure (dead port). |
| `tests/integration/test_api.py` | HTTP tests via `httpx.ASGITransport` with `svc.coordinator` monkeypatched to in-process shards: catalog/detail, upload (.md + hand-built PDF), query stream + cached stream, chaos down/none/wipe + repair, chaos 403. |
| `tests/unit/test_events.py`, `tests/unit/test_text_extraction.py` | EventBus fan-out/history/drop-oldest, SSE framing; text extraction and doc_id slugs. |
| `tests/integration/test_repair.py` | 3 in-process shards + `CoordinatorState`: upsert, query dedup, repair after crash/restart, no resurrection of deleted docs. |
| `tests/chaos/chaos_test.py` | Script (not pytest): 3 shard subprocesses (ports 50151-3), ingest via `CoordinatorState.ingest`, SIGKILL shard_1 during a 50-query burst, repair, restart, repair, assert placement == ring. |
| `tests/load/locustfile.py` | Locust: 4:1 query:ingest against the coordinator. |
| `scripts/seed_documents.py` | POSTs 4 sample docs to a running coordinator. |
| `scripts/generate_pdf_guide.py` | `[input.md] [output.html]`, default `docs/ARCHITECTURE.md` → `SYSTEM_ARCHITECTURE_GUIDE.html` (printable). |
| `SYSTEM_ARCHITECTURE_GUIDE.html`, `yo.pdf` | Older generated guide artifacts (not source; may be out of date). |
| `Dockerfile` | Multi-target: `shard` (requirements-shard.txt only) and `coordinator` (CPU torch, full requirements, embedding model pre-downloaded). |
| `docker-compose.yml`, `docker/prometheus.yml` | Coordinator (with `SHARD_NODES` using service names), 3 shards, redis, prometheus. |
| `requirements.txt` / `requirements-shard.txt` | Full stack (includes `-r requirements-shard.txt`) / shard runtime only. |
| `frontend/` | Vite + React + TS UI (deps: react, react-dom; dev: vite, typescript, vitest, jsdom, @testing-library/react). `npm run dev` / `build` / `test` / `typecheck`. |
| `frontend/src/lib/auth.ts` | API key in localStorage (`rag.apiKey`), `authHeaders()`, and the `rag:auth-required` window event fired on any 401. |
| `frontend/src/lib/chatHistory.ts` | `Turn` type, `loadTurns()` / `saveTurns()` in localStorage (`rag.chat.v1`, last 30 turns, halves on quota errors). Turns that were mid-stream load as `stopped`. |
| `frontend/src/lib/api.ts` | Typed API client (adds the bearer key, notifies on 401), `eventsUrl(key)` (key goes in `?api_key=` since EventSource can't send headers), `createSseParser()`, `streamQuery()` (POST + streamed body; EventSource can't POST). |
| `frontend/src/hooks/useClusterEvents.ts` | `useClusterEvents(apiKey)`: one `EventSource` on `/cluster/events` (reopened when the key changes; on error it probes `/cluster/status` so a 401 reaches the sign-in screen) → `{status, events (dedup by id, newest first, max 200), connected, catalogVersion}`. `catalogVersion` bumps on events that can change the doc list. |
| `frontend/src/App.tsx` | Shell: top bar (health from the event stream), hash-routed tabs `#/ask`, `#/documents`, `#/cluster`. The Ask view stays mounted (hidden) so the conversation survives tab switches. Shows `SignIn` instead of the app after a 401; Sign out clears the key. Below 960px the side panel is a slide-over opened by the **Shards** button (Escape/backdrop closes it). `ShardsContext` gives every shard a stable color slot. |
| `frontend/src/components/` | `ChatView` (streaming turns, stop, citation chips, saved history + Clear conversation), `SignIn` (checks the key with `/cluster/status` before storing it), `AnswerParts` (`AnswerText`, `SourceCard`, `TelemetryStrip`), `DocumentsView` (upload, paste, catalog table, detail drawer, two-step delete), `ClusterView` (stat tiles, `RingChart`, `ShardCard` + chaos controls, `EventFeed`), `ui.tsx` (badges, pills, panels), `Icon.tsx` (inline SVG). |
| `frontend/src/test/helpers.ts`, `**/*.test.tsx` | Component tests (vitest, jsdom per file via `// @vitest-environment jsdom`): `ChatView` (stream, error, save/restore/clear, mid-stream restore), `AnswerParts` (telemetry pills, citations), `App` (401 → sign in → sign out, side panel toggle). Fakes: `mockFetch`, `sseResponse`, `FakeEventSource`. |
| `frontend/src/styles.css` | Design tokens with light + dark themes (`prefers-color-scheme`), 6 shard color slots, responsive breakpoints at 960px and 640px. |

## 3. REST API (coordinator)

| Method / path | Body | Returns |
|---|---|---|
| `POST /api/v1/documents/ingest` | `{doc_id, text, metadata?, replication_factor?}` (rf default `REPLICATION_FACTOR`, 1-16) | `{doc_id, doc_version, chunks_created, target_shards[], failed_shards{shard: reason}, status: "success"\|"partial"}`; 502 if no target acked, 503 if ring empty |
| `DELETE /api/v1/documents/{doc_id}` | none | `{doc_id, deleted_chunks{shard: n}, failed_shards{}}` |
| `POST /api/v1/query` | `{query, top_k=DEFAULT_TOP_K (1-50)}` | `{query, answer, sources[{chunk_id,doc_id,text,score,shard_id,metadata}], telemetry, provider_used, cached}` |
| `POST /api/v1/documents/upload` | multipart: `file` (.txt/.md/.pdf…), `doc_id?` (default: filename slug), `replication_factor?`, `metadata?` (JSON object string) | ingest response + `characters`; 413 too big (`MAX_UPLOAD_MB`), 415 unsupported type, 422 unreadable / no text. Metadata gets `source=upload, filename, content_type, size_bytes`. |
| `GET /api/v1/documents?limit=100&offset=0` | none | `{total, limit, offset, documents[{doc_id, doc_version, ingested_at, chunk_count, replication_factor, metadata, target_shards, replicas[{shard_id, doc_version, stale}], replication_status: ok\|under_replicated\|over_replicated}], errors[]}`, newest first |
| `GET /api/v1/documents/{doc_id}` | none | catalog entry + `chunks[{chunk_id, chunk_index, text}]` + `served_by`; 404 if no healthy shard has it |
| `POST /api/v1/query/stream` | same as `/query` | SSE: `retrieval {sources, telemetry, cached}` → `token {text}`… (`reset {}` = drop text so far) → `done {provider_used, model_name, latency_ms, cached}`; `error {message}` on failure. Cache hit = one `token` with the whole answer. |
| `GET /api/v1/cluster/status` | none | `{total_nodes, healthy_nodes, replication_factor, ring_distribution, nodes[{shard_id,address,healthy,vector_count,ring_share_pct,fault{mode,latency_ms}}], last_repair, chaos_enabled}` |
| `GET /api/v1/cluster/events` | none | SSE: on connect one `cluster_status` + replay of `history` (with ids); then live `cluster_status` (each heartbeat, only while someone is subscribed), `node_down`/`node_up {shard_id, address}`, `repair_completed {summary}` (only when it changed something or errored), `document_ingested {doc_id, doc_version, chunks, target_shards, failed_shards}`, `document_deleted`, `fault_injected`. Every payload has `timestamp`. `: keepalive` after 15s idle. |
| `GET /api/v1/chaos` | none | `{enabled, shards{id: {mode, latency_ms}}}` |
| `POST /api/v1/chaos/shards/{shard_id}` | `{mode: down\|slow\|wipe\|none, latency_ms}` | 403 unless `CHAOS_ENABLED`; runs a heartbeat right away; `wipe` also requests repair |
| `POST /api/v1/cluster/repair` | none | repair summary `{copied, trimmed, tombstoned, errors[], documents_seen, shards[], duration_ms, finished_at}` |
| `GET /metrics` | none | Prometheus text |
| `GET /` | none | Built UI (`frontend/dist`), mounted last, only if the build exists |

Query `telemetry`: `total_scatter_latency_ms, shards_queried, shards_successful, shards_registered, candidates_retrieved, unique_results, degraded, errors[], shard_metrics{shard: {status, latency_ms, items_retrieved, error?}}`.
`shards_registered > shards_queried` means some shards were out of the ring (down) when the query ran; that isn't counted as `degraded`.
Each source also has `replica_shards[]`: the other shards that returned the same chunk, which the merge collapsed into one.

## 4. gRPC contract (`proto_gen/vector_service.proto`, package `vector_service`)

`VectorStoreService`:
- `IngestVector(IngestVectorRequest{shard_id, chunks[VectorChunk], is_replica_sync})` → `{success, inserted_count, message}`.
  Chunks are grouped by `doc_id`; each group **replaces** the stored doc unless the stored `doc_version` is newer (stale write skipped, still `success=true`).
- `SearchVector(SearchRequest{shard_id, query_vector[], top_k})` → `{results[SearchResultItem(+doc_version)], shard_id, latency_ms}`. Wrong dimension → `INVALID_ARGUMENT`; failure → `INTERNAL`.
- `HealthCheck(HealthCheckRequest{client_id})` → `{shard_id, healthy, vector_count, uptime_seconds}`
- `DeleteDocument({doc_id, below_version})` → `{deleted_count}` (`below_version` 0 = unconditional)
- `ListDocuments({})` → `{documents[DocumentSummary{doc_id, doc_version, chunk_count, replication_factor}]}`
- `ListDocuments` summaries also carry `metadata_json` (doc-level metadata, `chunk_index` removed).
- `FetchDocument({doc_id, omit_embeddings})` → `{chunks[VectorChunk]}`, with embeddings reconstructed from FAISS unless omitted; `NOT_FOUND` if absent.
- `SetFault({mode, latency_ms})` → `{mode, latency_ms, vector_count}`. `PERMISSION_DENIED` unless the shard runs with `CHAOS_ENABLED`. `wipe` clears the index once (mode unchanged).

`VectorChunk` = `{chunk_id, doc_id, text, chunk_index, metadata_json, embedding[], doc_version, replication_factor}`.
`doc_version` is `time.time_ns()` at the coordinator when ingested.

Regenerate stubs (from repo root):
```bash
python -m grpc_tools.protoc -I proto_gen --python_out=proto_gen --grpc_python_out=proto_gen proto_gen/vector_service.proto
# then fix the import in vector_service_pb2_grpc.py to: from proto_gen import vector_service_pb2 as vector__service__pb2
```

## 5. Data flows

### Ingest (`CoordinatorState.ingest`)
1. `rf = replication_factor or REPLICATION_FACTOR`; `version = time_ns()`; `targets = ring.get_nodes(doc_id, rf)` (ring = healthy nodes only).
2. Chunk → `embed_batch` in a thread → `VectorChunk`s carrying version + rf.
3. `IngestVector` to every target concurrently (timeout `max(5s, SHARD_TIMEOUT_SECONDS)`); check `resp.success`.
4. If ≥1 succeeded: `DeleteDocument(below_version=version)` on every other healthy shard (removes copies left by earlier ring layouts), clear tombstone, invalidate cache.
5. If any failed: set `repair_requested` (repair fills the missing replica). Endpoint returns `partial`, or 502 if none succeeded.

Sharding key is `doc_id` → all chunks of a document live together on the same N shards.

### Delete (`CoordinatorState.delete`)
Records `tombstones[doc_id] = time_ns()` (in coordinator memory), sends `DeleteDocument` to every healthy shard, invalidates cache.

### Query (`POST /query`)
1. Cache lookup (thread). Hit → `cached=True`.
2. `embed_text` in a thread.
3. Scatter `SearchVector(top_k)` to **all healthy shards**, per-shard timeout `SHARD_TIMEOUT_SECONDS`.
4. `merge_results`: drop hits whose `doc_version` is older than the newest seen for that doc → dedup by `chunk_id` (keep best score, record the other shards in `replica_shards`) → drop `score < SCORE_THRESHOLD` → sort → `top_k`.
5. LLM in a thread → format sources → observe latency → cache set **only if not degraded**.

### Heartbeat / membership
- Lifespan registers nodes from `SHARD_NODES`; channels use capped reconnect backoff (200ms → 2s) so restarts are noticed quickly.
- Every `HEARTBEAT_INTERVAL_SECONDS` (2s) `check_node_health()` probes all nodes **concurrently** (1s timeout), then updates state under `_lock`:
  fail & was healthy → remove from ring; success & was unhealthy → add to ring. Any change sets `repair_requested`.

### Streaming query (`POST /query/stream`)
Retrieval as above → `retrieval` event → `llm_generator.stream_answer` runs in a worker thread and hands events to the
event loop through an `asyncio.Queue` (`loop.call_soon_threadsafe`). If the client disconnects, the generator's `finally`
sets a `threading.Event` that the provider loop checks between tokens. Claude streams via `client.beta.messages.stream` with
the same refusal fallback (it continues on the same stream); a final refusal or provider error → `reset` + mock answer.
The full answer is cached after `done` unless degraded.

### Events
`EventBus.publish()` is called from heartbeats (node_up/down), ingest, delete, repair (when non-trivial), `set_fault`, and the
heartbeat loop (status snapshot, only when there are subscribers). SSE subscribers each get a bounded queue.

### Fault injection (dev-only)
`POST /chaos/shards/{id}` → shard `SetFault`. `down`: every RPC except SetFault aborts `UNAVAILABLE`, so heartbeats mark it
unhealthy → ring removal → repair. The process and its TCP connection stay up, so this simulates an unresponsive shard,
not a dead socket. `slow`: per-RPC sleep (searches past `SHARD_TIMEOUT_SECONDS` show up as timeouts). `wipe`: empties
the index, simulating a crash-restart. `none`: back to normal.

### Repair (`CoordinatorState.repair`, anti-entropy)
Runs when `repair_requested` is set, every `REPAIR_INTERVAL_SECONDS` (30s; 0 = only on request), or via `POST /cluster/repair`. One pass at a time (`_repair_lock`).
1. `ListDocuments` on every healthy shard → `holders[doc_id][shard] = summary`.
2. Per doc: `latest` = max version. If a tombstone is newer than `latest` → delete everywhere (`below_version=tombstone`).
3. `targets = ring.get_nodes(doc_id, rf of latest copy)`; `missing` = targets without the latest version.
   `FetchDocument` from a fresh holder → `IngestVector(is_replica_sync=True)` to the missing targets.
4. Once all targets hold the latest version, `DeleteDocument(below_version=latest+1)` on holders that aren't targets (trim).
5. Invalidate cache if anything changed; store summary in `last_repair`.

Result: after a crash, survivors get extra copies to restore N; when the shard returns (empty, in-memory) it gets its share back and the temporary copies are trimmed.

## 6. Config & observability

Env vars (`shared/config.py`):
`SHARD_NODES` (`shard_0=localhost:50051,shard_1=localhost:50052,shard_2=localhost:50053`), `REPLICATION_FACTOR` (2),
`VIRTUAL_NODES` (100), `SHARD_TIMEOUT_SECONDS` (1.5), `HEARTBEAT_INTERVAL_SECONDS` (2), `REPAIR_INTERVAL_SECONDS` (30),
`EMBEDDING_MODEL_NAME` (all-MiniLM-L6-v2), `EMBEDDING_DIM` (384), `DEFAULT_CHUNK_SIZE` (500), `DEFAULT_CHUNK_OVERLAP` (50),
`DEFAULT_TOP_K` (5), `SCORE_THRESHOLD` (0.0), `LLM_PROVIDER` (mock|gemini|claude), `GEMINI_API_KEY`, `GEMINI_MODEL` (gemini-2.5-flash),
`ANTHROPIC_API_KEY`, `CLAUDE_MODEL` (claude-opus-5-5), `CLAUDE_EFFORT` (medium), `REDIS_HOST`, `REDIS_PORT`, `CACHE_TTL_SECONDS` (300),
`TRACING_EXPORTER` (none|console), `MAX_UPLOAD_MB` (10), `CHAOS_ENABLED` (false; coordinator and shards), `API_KEY` (empty = no auth). All are consumed.

Auth: when `API_KEY` is set, the `require_api_key` middleware in `coordinator_service.py` returns 401 for any `/api/` request (except `OPTIONS`) without `Authorization: Bearer <key>` or `X-API-Key: <key>`; `GET /api/v1/cluster/events` also accepts `?api_key=`. Compared with `hmac.compare_digest`. CORS wraps the middleware so 401s carry CORS headers. `/metrics` and the UI at `/` stay open.

Prometheus metrics: `rag_query_latency_seconds` (hist), `rag_cache_hits_total{status}`,
`rag_shard_request_duration_seconds{shard_id,status=success|timeout|error}` (hist, observed per shard search),
`rag_node_health_status{shard_id}`, `rag_documents_ingested_total`, `rag_repair_operations_total{action=copied|trimmed}`.

OpenTelemetry: TracerProvider (console exporter when `TRACING_EXPORTER=console`, otherwise spans are dropped),
FastAPI instrumentor (excludes `/metrics`), grpc.aio client instrumentor (coordinator) and server instrumentor (shards) so the trace
continues into each shard, manual spans `embed_batch`, `embed_query`, `llm_generate`. No OTLP exporter yet.

## 7. Running

```bash
pip install -r requirements.txt
python shard_node/shard_service.py --shard-id shard_0 --port 50051   # ×3 (50052, 50053)
python coordinator/coordinator_service.py                             # :8000
python scripts/seed_documents.py
(cd frontend && npm install && npm run dev)                          # UI on :5173 (or `npm run build`, then the coordinator serves it on :8000)
python -m pytest -v                                                   # unit + integration (pytest.ini)
python tests/chaos/chaos_test.py
locust -f tests/load/locustfile.py --host=http://localhost:8000
docker compose up --build
```
First run downloads the MiniLM model (~90MB); the coordinator Docker image bakes it in.

## 8. Design properties (as implemented)

- **Write path**: consistent-hash routed, replication N (default 2), success if ≥1 target acks (reported as `partial`); repair converges the rest. No write quorum.
- **Versioning**: last-writer-wins by coordinator timestamp (`time_ns`). Shards reject older versions. Single coordinator, so clocks don't conflict.
- **Read path**: broadcast to every healthy shard, drop stale versions, dedup by `chunk_id`, merge by score.
- **Placement**: converges to the ring through repair (copy then trim). Ring-routed reads would be correct after a repair pass.
- **Durability**: none on disk. Vectors live in shard memory; a lost shard is refilled from surviving replicas. Losing all N replicas of a doc loses it.
- **Coordinator**: single instance, single point of failure; ring, node health and tombstones are in its memory.

# Architecture — Distributed RAG System

> Source of truth for *how the code works today*. Keep it in sync with the code.
> Last verified against code: 2026-10-29 (working tree on top of `621471e`, uncommitted).

## 1. What it is

A sharded, replicated vector-search + RAG backend in Python:

- **Coordinator** (FastAPI, port 8000): chunks and embeds documents, routes them to shards via a
  consistent hash ring, fans queries out to all healthy shards over gRPC, merges and de-duplicates results, calls an LLM,
  caches answers in Redis, and runs a heartbeat loop plus an anti-entropy **repair loop**.
- **Shard nodes** (gRPC aio, ports 50051-50053): each holds an **in-memory** FAISS index plus per-document
  version info. Plain storage: no embedding, no hashing, no knowledge of other shards.
- **Redis**: query-result cache (falls back to an in-process dict with TTL if Redis is unreachable).
- **Prometheus**: scrapes the coordinator's `/metrics`.
- **Frontend**: single `frontend/index.html` (React 18.3.1 UMD + Babel standalone 7.26.10, pinned), talks to `http://localhost:8000`.

```
Browser (frontend/index.html)
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
   └─ Prometheus /metrics + OpenTelemetry (shared/telemetry.py)
   │ gRPC (proto_gen/vector_service.proto)
   ▼
Shard nodes ×3 (shard_node/shard_service.py) → FAISSVectorStore (shared/faiss_store.py)
```

## 2. File map

| Path | Role |
|---|---|
| `coordinator/coordinator_service.py` | FastAPI app (lifespan handler), `CoordinatorState` (nodes, ring, cache, tombstones, `ingest`, `delete`, `repair`, `check_node_health`), heartbeat + repair loops, REST endpoints. Entry point. |
| `coordinator/scatter_gather.py` | `ScatterGatherEngine.execute_scatter_gather()`: parallel `SearchVector`, per-shard timeout, observes `rag_shard_request_duration_seconds`. `merge_results()` (static): drop stale doc versions → dedup by `chunk_id` → score threshold → sort → top_k. |
| `coordinator/cache.py` | `QueryCache`: key `rag:cache:g{generation}:md5(lower(query)_top{k})`, TTL `CACHE_TTL_SECONDS`. `invalidate_all()` = `INCR rag:cache:generation` (O(1)). |
| `coordinator/baseline_rag.py` | Standalone single-node RAG FastAPI app (no shards/gRPC). Reference/benchmark baseline. Also port 8000. |
| `shard_node/shard_service.py` | `VectorStoreServicer`: IngestVector (per-doc upsert, version-checked), SearchVector, HealthCheck, DeleteDocument, ListDocuments, FetchDocument. Validates embedding dimension; search errors return gRPC status codes. CLI: `--shard-id`, `--port`. |
| `shared/config.py` | `SystemConfig` read from env vars; `config.shard_nodes()` parses `SHARD_NODES`. Singleton `config`. |
| `shared/consistent_hash.py` | `ConsistentHashRing`: MD5 → first 32 bits, vnodes `"{node}#vnode_{i}"`, sorted list + cached hash list for bisect, clockwise walk for distinct replicas. `ring_distribution()` = % of the key space owned (arc length). |
| `shared/document_processor.py` | `DocumentChunk` model (uuid `chunk_id`), `DocumentProcessor` char-based chunking (default 500/50) snapped to spaces. |
| `shared/embeddings.py` | `EmbeddingsEngine` lazy, thread-safe model load (`EMBEDDING_MODEL_NAME`); `embed_text`, `embed_batch`. Singleton `embeddings_engine`. |
| `shared/faiss_store.py` | `FAISSVectorStore`: `IndexIDMap2(IndexFlatIP)`, int-id ↔ chunk, `doc_to_ids`, `docs: doc_id → DocumentInfo(version, replication_factor, chunk_count)`. `upsert_document`, `add_chunks`, `search`, `delete_by_doc_id(below_version)`, `list_documents`, `get_document` (reconstructs embeddings), `count`, `clear`. Raises `ValueError` on dimension mismatch. |
| `shared/llm.py` | `LLMGenerator`: provider from `LLM_PROVIDER` + key present, else mock. Claude: `CLAUDE_MODEL` (default `claude-opus-5-5`), `CLAUDE_EFFORT`, server-side refusal fallback (`fallbacks="default"`, beta `server-side-fallback-2026-07-01`). Gemini: `GEMINI_MODEL`. Any API error or refusal → mock. |
| `shared/telemetry.py` | Prometheus metrics (§6), `setup_telemetry(exporter)`, `instrument_fastapi`, `instrument_grpc_client`, `instrument_grpc_server`, `span()` context manager. |
| `proto_gen/` | Generated gRPC code + `.proto` (generated with grpcio-tools 1.85 → needs grpcio ≥ 1.85, protobuf ≥ 7.35.1). |
| `tests/conftest.py`, `pytest.ini` | Adds repo root to `sys.path`, `asyncio_mode=auto`, forces mock LLM and an unreachable Redis (memory cache). |
| `tests/unit/` | `test_consistent_hash.py`, `test_baseline.py` (chunking, embeddings+FAISS, mock LLM), `test_faiss_store.py` (upsert, stale writes, delete-below-version, export, dim checks, merge dedup). |
| `tests/integration/test_scatter_gather.py` | In-process gRPC servers; ingest/search and partial failure (dead port). |
| `tests/integration/test_repair.py` | 3 in-process shards + `CoordinatorState`: upsert, query dedup, repair after crash/restart, no resurrection of deleted docs. |
| `tests/chaos/chaos_test.py` | Script (not pytest): 3 shard subprocesses (ports 50151-3), ingest via `CoordinatorState.ingest`, SIGKILL shard_1 during a 50-query burst, repair, restart, repair, assert placement == ring. |
| `tests/load/locustfile.py` | Locust: 4:1 query:ingest against the coordinator. |
| `scripts/seed_documents.py` | POSTs 4 sample docs to a running coordinator. |
| `scripts/generate_pdf_guide.py` | `[input.md] [output.html]`, default `docs/ARCHITECTURE.md` → `SYSTEM_ARCHITECTURE_GUIDE.html` (printable). |
| `SYSTEM_ARCHITECTURE_GUIDE.html`, `yo.pdf` | Older generated guide artifacts (not source; may be out of date). |
| `Dockerfile` | Multi-target: `shard` (requirements-shard.txt only) and `coordinator` (CPU torch, full requirements, embedding model pre-downloaded). |
| `docker-compose.yml`, `docker/prometheus.yml` | Coordinator (with `SHARD_NODES` using service names), 3 shards, redis, prometheus. |
| `requirements.txt` / `requirements-shard.txt` | Full stack (includes `-r requirements-shard.txt`) / shard runtime only. |
| `frontend/index.html` | Chat UI + ingest form + topology panel polling `/cluster/status` every 2s. |

## 3. REST API (coordinator)

| Method / path | Body | Returns |
|---|---|---|
| `POST /api/v1/documents/ingest` | `{doc_id, text, metadata?, replication_factor?}` (rf default `REPLICATION_FACTOR`, 1-16) | `{doc_id, doc_version, chunks_created, target_shards[], failed_shards{shard: reason}, status: "success"\|"partial"}`; 502 if no target acked, 503 if ring empty |
| `DELETE /api/v1/documents/{doc_id}` | none | `{doc_id, deleted_chunks{shard: n}, failed_shards{}}` |
| `POST /api/v1/query` | `{query, top_k=DEFAULT_TOP_K (1-50)}` | `{query, answer, sources[{chunk_id,doc_id,text,score,shard_id,metadata}], telemetry, provider_used, cached}` |
| `GET /api/v1/cluster/status` | none | `{total_nodes, healthy_nodes, replication_factor, ring_distribution, nodes[{shard_id,address,healthy,vector_count,ring_share_pct}], last_repair}` |
| `POST /api/v1/cluster/repair` | none | repair summary `{copied, trimmed, tombstoned, errors[], documents_seen, shards[], duration_ms, finished_at}` |
| `GET /metrics` | none | Prometheus text |

Query `telemetry`: `total_scatter_latency_ms, shards_queried, shards_successful, candidates_retrieved, unique_results, degraded, errors[], shard_metrics{shard: {status, latency_ms, items_retrieved, error?}}`.

## 4. gRPC contract (`proto_gen/vector_service.proto`, package `vector_service`)

`VectorStoreService`:
- `IngestVector(IngestVectorRequest{shard_id, chunks[VectorChunk], is_replica_sync})` → `{success, inserted_count, message}`.
  Chunks are grouped by `doc_id`; each group **replaces** the stored doc unless the stored `doc_version` is newer (stale write skipped, still `success=true`).
- `SearchVector(SearchRequest{shard_id, query_vector[], top_k})` → `{results[SearchResultItem(+doc_version)], shard_id, latency_ms}`. Wrong dimension → `INVALID_ARGUMENT`; failure → `INTERNAL`.
- `HealthCheck(HealthCheckRequest{client_id})` → `{shard_id, healthy, vector_count, uptime_seconds}`
- `DeleteDocument({doc_id, below_version})` → `{deleted_count}` (`below_version` 0 = unconditional)
- `ListDocuments({})` → `{documents[DocumentSummary{doc_id, doc_version, chunk_count, replication_factor}]}`
- `FetchDocument({doc_id})` → `{chunks[VectorChunk]}` with embeddings (reconstructed from FAISS); `NOT_FOUND` if absent.

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
4. `merge_results`: drop hits whose `doc_version` is older than the newest seen for that doc → dedup by `chunk_id` (keep best score) → drop `score < SCORE_THRESHOLD` → sort → `top_k`.
5. LLM in a thread → format sources → observe latency → cache set **only if not degraded**.

### Heartbeat / membership
- Lifespan registers nodes from `SHARD_NODES`; channels use capped reconnect backoff (200ms → 2s) so restarts are noticed quickly.
- Every `HEARTBEAT_INTERVAL_SECONDS` (2s) `check_node_health()` probes all nodes **concurrently** (1s timeout), then updates state under `_lock`:
  fail & was healthy → remove from ring; success & was unhealthy → add to ring. Any change sets `repair_requested`.

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
`TRACING_EXPORTER` (none|console). All are consumed.

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
# open frontend/index.html
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

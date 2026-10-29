# Distributed RAG System (Sharded Vector Search, Consistent Hashing & gRPC Scatter-Gather)

A production-grade, fault-tolerant **Distributed Retrieval-Augmented Generation (RAG)** infrastructure built in Python using FastAPI, gRPC, FAISS, Redis, OpenTelemetry, Prometheus, and a single-page React UI.

This project implements the foundational distributed systems primitives that power real-world vector databases (Pinecone, Milvus, Qdrant) and enterprise RAG platforms.

---

## System Architecture

```
                                  +-----------------------+
                                  | React UI / Client API |
                                  +-----------+-----------+
                                              |
                                     HTTP REST / JSON
                                              |
                                              v
                                  +-----------------------+
                                  |   Coordinator Node    | <---> [ Redis Cache ]
                                  | (FastAPI / Scatter-   | <---> [ Prometheus Metrics ]
                                  |     Gather Router)    |
                                  +---+-------+-------+---+
                                      |       |       |
                     +----------------+       |       +----------------+
                     | gRPC                   | gRPC                   | gRPC
                     v                        v                        v
            +----------------+       +----------------+       +----------------+
            |  Shard Node 0  |       |  Shard Node 1  |       |  Shard Node 2  |
            | (FAISS Index + |       | (FAISS Index + |       | (FAISS Index + |
            |  gRPC Server)  |       |  gRPC Server)  |       |  gRPC Server)  |
            +----------------+       +----------------+       +----------------+
               Primary S0               Primary S1               Primary S2
               Replica S2               Replica S0               Replica S1
```

---

## Key Features & Core Distributed Systems Concepts

1. **Hand-Rolled Consistent Hash Ring** (`shared/consistent_hash.py`):
   - Implemented from scratch using MD5 32-bit integer ring mapping (`[0, 2^32 - 1]`).
   - Uses $V=100$ virtual nodes per physical shard to enforce uniform key distribution ($33.33\% \pm 2\%$).
   - Minimizes key remapping churn on node join/leave ($\sim 1/N$ remapped vs $\sim (N-1)/N$ in static `hash % N`).

2. **gRPC Scatter-Gather Concurrent Fan-Out** (`coordinator/scatter_gather.py`):
   - Coordinator fans query out across active shard nodes concurrently using `asyncio.gather` over gRPC.
   - Enforces per-shard timeouts ($1.5s$) to isolate slow/dead nodes without blocking the entire request.
   - Merges candidate chunks across shards, de-duplicates replica hits by `chunk_id`, drops chunks from stale document versions, and re-ranks top-$K$ by inner-product similarity score.

3. **Replication, Upserts & Anti-Entropy Repair** ($N=2$):
   - Each document is written to its primary and replica shards; the ingest response reports which shards acked (`success` / `partial`).
   - Re-ingesting a `doc_id` is an upsert: every write carries a version and shards ignore older versions.
   - A repair loop (on every membership change, plus every 30s) copies missing or stale replicas and trims copies the ring no longer assigns, so a crashed shard is re-filled when it comes back empty.
   - Reads fan out to every healthy shard (AP bias), so they survive the loss of any one replica.

4. **Heartbeat Failure Detection & Self-Healing Ring**:
   - Background monitor pings gRPC `HealthCheck` endpoints every $2s$.
   - Automatically removes dead shards from the ring, restores them upon recovery, and triggers a repair pass.

5. **Redis Caching & Invalidation**:
   - Query result cache with TTL. Invalidation on ingest/delete is O(1): keys carry a generation number that gets bumped.

6. **End-to-End Observability**:
   - OpenTelemetry spans for REST requests (FastAPI instrumentor), every coordinator→shard gRPC call (client + server instrumentors, context propagated), embedding and LLM calls. Set `TRACING_EXPORTER=console` to print them.
   - Prometheus metrics at `/metrics`: query latency, cache hits, per-shard request latency, node health, ingests, repair operations.

7. **Automated Chaos Testing** (`tests/chaos/chaos_test.py`):
   - Starts 3 shard processes, ingests documents, SIGKILLs a shard while a burst of queries is in flight, and checks every query is answered from replicas. Then it restarts the shard empty and verifies repair puts every document back on its ring-assigned replicas.

---

## Tech Stack

- **Service Framework**: Python 3.11+, FastAPI, asyncio, uvicorn
- **Inter-Service Communication**: gRPC with Protocol Buffers (`proto_gen/vector_service.proto`)
- **Embeddings**: Local `sentence-transformers/all-MiniLM-L6-v2` (384-dimensional dense vectors)
- **Vector Search Engine**: FAISS (`IndexFlatIP` with L2 normalization for exact cosine similarity)
- **Sharding**: Pure Python Consistent Hash Ring with virtual nodes
- **Cache**: Redis (TTL + generation-based invalidation; in-process fallback when Redis is down)
- **LLM Generator**: Swappable interface (Google Gemini, Anthropic Claude, or local Mock fallback)
- **Observability**: OpenTelemetry (FastAPI + gRPC instrumentation) + Prometheus
- **Frontend UI**: Vite + React 18 + TypeScript console (`frontend/`): streaming chat, document manager, live cluster view with failure controls
- **Testing**: `pytest`, `pytest-asyncio`, Locust (load testing), custom Chaos script

---

## Quickstart Guide

### 1. Installation
```bash
git clone <repo-url>
cd "Distributed RAG System"
pip install -r requirements.txt
```

### 2. Run Microservices

**Terminal 1: Shard Node 0 (gRPC port 50051)**
```bash
python shard_node/shard_service.py --shard-id shard_0 --port 50051
```

**Terminal 2: Shard Node 1 (gRPC port 50052)**
```bash
python shard_node/shard_service.py --shard-id shard_1 --port 50052
```

**Terminal 3: Shard Node 2 (gRPC port 50053)**
```bash
python shard_node/shard_service.py --shard-id shard_2 --port 50053
```

**Terminal 4: Coordinator REST API (port 8000)**
```bash
python coordinator/coordinator_service.py
# Shard addresses come from SHARD_NODES (default: shard_0=localhost:50051,shard_1=localhost:50052,shard_2=localhost:50053)
```

**Or everything at once with Docker:**
```bash
docker compose up --build   # coordinator, 3 shards, redis, prometheus
```

**Seed sample documents:**
```bash
python scripts/seed_documents.py
```

### Configuration (env vars)
| Variable | Default | Meaning |
|---|---|---|
| `SHARD_NODES` | `shard_0=localhost:50051,...` | Shards the coordinator manages |
| `REPLICATION_FACTOR` | `2` | Default copies per document |
| `REPAIR_INTERVAL_SECONDS` | `30` | Periodic repair pass (0 = only on membership change) |
| `LLM_PROVIDER` | `mock` | `mock`, `gemini` or `claude` (needs `GEMINI_API_KEY` / `ANTHROPIC_API_KEY`) |
| `CLAUDE_MODEL` / `GEMINI_MODEL` | `claude-opus-5-5` / `gemini-2.5-flash` | Model used by each provider |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Query cache |
| `TRACING_EXPORTER` | `none` | `console` prints OpenTelemetry spans |
| `MAX_UPLOAD_MB` | `10` | Upload size limit |
| `CHAOS_ENABLED` | `false` | Dev-only fault injection endpoints (set on coordinator **and** shards) |
| `API_KEY` | _(empty)_ | If set, every `/api/v1` call needs `Authorization: Bearer <key>` (or `X-API-Key`); the UI asks for it on a sign-in screen. `/metrics` and the UI files stay open |

See `docs/ARCHITECTURE.md` for the full list.

### 3. Web UI
```bash
cd frontend && npm install
npm run dev        # http://localhost:5173, proxies /api to the coordinator on :8000 (override with COORDINATOR_URL)
# or build once and let the coordinator serve it at http://localhost:8000/
npm run build
```
The UI has three views:
- **Ask**: answers stream in token by token. Each answer shows its source cards (score, the shard that served the chunk, and replicas that returned it too) and per-shard latency for that query.
- **Documents**: drag-and-drop `.txt`/`.md`/`.pdf` upload or pasted text, the catalog with replica placement and replication health, a chunk viewer, and delete.
- **Cluster**: the hash-ring share chart, shard health and vector counts, the live activity feed (Server-Sent Events), Run repair, and, with `CHAOS_ENABLED=true`, buttons to take a shard down, slow it, or wipe it and watch failover and repair happen.

`docker compose up --build` builds the UI into the coordinator image, so it's served at http://localhost:8000/.

The conversation is saved in the browser (localStorage, last 30 questions) and survives a reload; **Clear conversation** wipes it. When the coordinator has `API_KEY` set, the UI shows a sign-in screen and keeps the key in localStorage until you sign out. On screens narrower than 960px, the shard panel opens from the **Shards** button.

UI tests: `npm test` runs vitest (lib unit tests and component tests with jsdom + Testing Library).

---

## REST API

| Method & path | What it does |
|---|---|
| `POST /api/v1/documents/ingest` | Ingest `{doc_id, text, metadata?, replication_factor?}` |
| `POST /api/v1/documents/upload` | Multipart upload of `.txt`/`.md`/`.pdf` (`file`, optional `doc_id`, `replication_factor`, `metadata` JSON) |
| `GET /api/v1/documents` | Document catalog: version, chunks, metadata, replicas, replication status (`ok` / `under_replicated` / `over_replicated`) |
| `GET /api/v1/documents/{doc_id}` | One document with its chunk texts |
| `DELETE /api/v1/documents/{doc_id}` | Delete everywhere |
| `POST /api/v1/query` | Answer + sources + per-shard telemetry |
| `POST /api/v1/query/stream` | Same, as Server-Sent Events: `retrieval` → `token`… (`reset`) → `done` (or `error`) |
| `GET /api/v1/cluster/status` | Nodes, health, ring share, faults, last repair |
| `GET /api/v1/cluster/events` | SSE stream: `cluster_status` every heartbeat, `node_down`/`node_up`, `repair_completed`, `document_ingested`/`document_deleted`, `fault_injected` |
| `POST /api/v1/cluster/repair` | Run a repair pass now |
| `GET /api/v1/chaos`, `POST /api/v1/chaos/shards/{id}` | Dev-only: `{"mode": "down" \| "slow" \| "wipe" \| "none", "latency_ms"}` |

Try streaming from a terminal:
```bash
curl -N -X POST localhost:8000/api/v1/query/stream -H 'content-type: application/json' -d '{"query":"what is consistent hashing?"}'
curl -N localhost:8000/api/v1/cluster/events
```

---

## Verification & Testing

### 1. Run Unit & Integration Test Suite
```bash
python -m pytest -v   # pytest.ini points at tests/unit and tests/integration
```

### 2. Run Automated Chaos Test (Shard Crash During Queries, Recovery & Repair)
```bash
python tests/chaos/chaos_test.py
```

### 3. Run Performance Load Test (Locust)
```bash
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

---

## Interview Deep Dive & Talking Points

### CAP Theorem Tradeoffs
- **AP Bias (Availability over Consistency)**: Distributed search engines prioritize returning low-latency partial results over blocking queries for strict global linearizability.
- **Why reads broadcast**: a semantic query isn't keyed by `doc_id`, so it can't be routed to a single replica set. Every healthy shard is searched, replica hits are de-duplicated, and losing one replica only costs that shard's latency budget. Versions on every chunk let the merge drop results from a stale replica.
- **Eventual consistency for writes**: a write succeeds if at least one target shard acks; the repair loop converges the remaining replicas.

### Consistent Hashing vs Static `hash % N`
- Under static `hash % N`, adding a single node causes $\frac{N-1}{N}$ (e.g. $75\%-90\%$) of all keys to re-map, causing massive cache misses and network re-indexing storms.
- Consistent hashing with virtual nodes ensures only $\frac{1}{N}$ ($\sim 25\%-33\%$) of keys migrate to the new node, keeping ring distribution balanced within $\pm 2\%$.

### Production Scaling Considerations (100x Growth)
- **Ingestion Queue**: Ingest is synchronous today; at higher scale it would move behind a durable log such as **Apache Kafka**.
- **Vector Storage**: In-memory FAISS per container transitions to distributed ANN indices (HNSW / IVF-PQ in Milvus / Pinecone).
- **Metadata & durability**: Shard indexes and metadata live in process memory today (repair restores lost replicas from surviving copies); next step is snapshotting to disk and a metadata store such as PostgreSQL.

# Distributed RAG System (Sharded Vector Search, Consistent Hashing & gRPC Scatter-Gather)

A production-grade, fault-tolerant **Distributed Retrieval-Augmented Generation (RAG)** infrastructure built in Python using FastAPI, gRPC, FAISS, Redis, OpenTelemetry, Prometheus, and React.

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
   - Merges candidate chunks across shards and re-ranks top-$K$ by inner-product similarity score.

3. **Replication & Quorum Reads** ($N=2$):
   - Each document is routed to primary and replica target shards.
   - Supports AP (Available/Partition-Tolerant) read strategies: `FASTEST_REPLICA` vs `READ_QUORUM`.

4. **Heartbeat Failure Detection & Self-Healing Ring**:
   - Background monitor pings gRPC `HealthCheck` endpoints every $2s$.
   - Automatically removes dead shards from the ring and restores them upon recovery.

5. **Redis Caching & Invalidation**:
   - Query result cache with TTL and explicit invalidation upon document ingestion.

6. **End-to-End Observability**:
   - OpenTelemetry tracing across REST $\rightarrow$ Coordinator $\rightarrow$ gRPC $\rightarrow$ FAISS $\rightarrow$ LLM.
   - Prometheus metrics endpoint at `/metrics`.

7. **Automated Mid-Query Chaos Testing** (`tests/chaos/chaos_test.py`):
   - Starts a 3-shard cluster, ingests documents, SIGKILLs a shard container mid-query, and verifies that queries succeed from replica nodes.

---

## Tech Stack

- **Service Framework**: Python 3.11+, FastAPI, asyncio, uvicorn
- **Inter-Service Communication**: gRPC with Protocol Buffers (`proto_gen/vector_service.proto`)
- **Embeddings**: Local `sentence-transformers/all-MiniLM-L6-v2` (384-dimensional dense vectors)
- **Vector Search Engine**: FAISS (`IndexFlatIP` with L2 normalization for exact cosine similarity)
- **Sharding**: Pure Python Consistent Hash Ring with virtual nodes
- **Cache & Queue**: Redis (TTL + explicit invalidation)
- **LLM Generator**: Swappable interface (Google Gemini, Anthropic Claude, or local Mock fallback)
- **Observability**: OpenTelemetry + Prometheus Exporter
- **Frontend UI**: Modern Glassmorphism React Chat & Topology Visualizer
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
```

### 3. Launch React Frontend UI
Open `frontend/index.html` directly in any modern web browser or serve via HTTP.

---

## Verification & Testing

### 1. Run Unit & Integration Test Suite
```bash
python -m pytest tests/unit/test_baseline.py tests/unit/test_consistent_hash.py tests/integration/test_scatter_gather.py -v
```

### 2. Run Automated Chaos Test (Mid-Query Node Crash & Recovery)
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
- **Read Quorum vs Fastest Replica**: For latency-sensitive workloads ($R=1$), queries hit the fastest replica. For strict read consistency ($R=2$), reads wait for quorum agreement.

### Consistent Hashing vs Static `hash % N`
- Under static `hash % N`, adding a single node causes $\frac{N-1}{N}$ (e.g. $75\%-90\%$) of all keys to re-map, causing massive cache misses and network re-indexing storms.
- Consistent hashing with virtual nodes ensures only $\frac{1}{N}$ ($\sim 25\%-33\%$) of keys migrate to the new node, keeping ring distribution balanced within $\pm 2\%$.

### Production Scaling Considerations (100x Growth)
- **Ingestion Queue**: At higher scale, Redis Streams transitions to **Apache Kafka** for partitioned, multi-subscriber log streaming.
- **Vector Storage**: In-memory FAISS per container transitions to distributed ANN indices (HNSW / IVF-PQ in Milvus / Pinecone).
- **Metadata**: In-memory dict transitions to PostgreSQL with read replicas.

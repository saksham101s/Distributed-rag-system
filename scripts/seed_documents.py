import requests

COORDINATOR_URL = "http://localhost:8000/api/v1/documents/ingest"

SAMPLE_DOCUMENTS = [
    {
        "doc_id": "doc_consistent_hashing",
        "text": "Consistent Hashing is a distributed systems technique used to assign keys across a set of shard nodes on a 32-bit integer ring space [0, 2^32 - 1]. Each physical node is assigned V virtual nodes on the ring using MD5 or MurmurHash3 to ensure uniform key distribution. When a node joins or leaves the cluster, consistent hashing ensures that only a minimal fraction of keys (1/N) are re-mapped to new nodes, preventing global cache churn and network re-indexing storms.",
        "replication_factor": 2
    },
    {
        "doc_id": "doc_scatter_gather",
        "text": "Scatter-Gather is a parallel query routing pattern used in distributed search engines. When a user issues a vector similarity query, the coordinator fans out SearchVector gRPC calls to all active shard nodes concurrently using HTTP/2 multiplexing. Each shard executes an in-memory FAISS vector search on its subset of data. The coordinator collects candidate chunks, isolates timed-out or dead nodes to preserve system availability (degraded state), and merges top-K items sorted by cosine similarity score.",
        "replication_factor": 2
    },
    {
        "doc_id": "doc_faiss_indexing",
        "text": "FAISS (Facebook AI Similarity Search) is a C++ library for efficient similarity search of dense vectors. In this system, each vector shard uses IndexFlatIP (Inner Product) wrapped in IndexIDMap2. By L2-normalizing the 384-dimensional sentence-transformer embeddings (all-MiniLM-L6-v2) prior to indexing, the exact inner product score computed by FAISS equals cosine similarity score.",
        "replication_factor": 2
    },
    {
        "doc_id": "doc_replication_cap",
        "text": "Replication in distributed RAG ensures fault tolerance by storing each document chunk across primary and replica shard nodes (Replication Factor N=2). In accordance with the CAP Theorem, retrieval platforms adopt an Available and Partition-Tolerant (AP) bias. Reads fan out to every healthy shard and replicas are de-duplicated by chunk id, so a query still succeeds when one replica is down. A background heartbeat monitor detects node crashes and updates the active ring, and an anti-entropy repair loop re-copies documents until each is back on its ring-assigned replicas.",
        "replication_factor": 2
    }
]

def seed():
    print("🌱 Seeding sample documents into Distributed RAG cluster...")
    for doc in SAMPLE_DOCUMENTS:
        try:
            res = requests.post(COORDINATOR_URL, json=doc)
            if res.status_code == 200:
                data = res.json()
                print(f"✅ Ingested '{doc['doc_id']}' -> Target Shards: {data['target_shards']}")
            else:
                print(f"❌ Failed to ingest '{doc['doc_id']}': {res.text}")
        except Exception as e:
            print(f"❌ Error connecting to coordinator: {e}")

if __name__ == "__main__":
    seed()

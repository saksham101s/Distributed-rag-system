import random
from locust import HttpUser, task, between

SAMPLE_QUERIES = [
    "How do distributed vector databases scale across shard nodes?",
    "What is the role of consistent hashing in vector sharding?",
    "How does gRPC scatter gather fanout handle partial node timeouts?",
    "Explain replication factor N=2 and read quorum consistency.",
    "What is the difference between IndexFlatIP and IndexIVFFlat in FAISS?",
    "How does Redis query caching accelerate retrieval latency?"
]

SAMPLE_DOCS = [
    ("doc_locust_1", "Raft consensus provides leader election and log replication for distributed coordinators."),
    ("doc_locust_2", "Consistent hashing maps virtual nodes to minimize data remapping during cluster expansion."),
    ("doc_locust_3", "OpenTelemetry distributed tracing context tracks query execution across REST and gRPC microservice boundaries.")
]


class RAGLoadUser(HttpUser):
    """Locust load test simulating concurrent users querying and ingesting into Distributed RAG system."""
    
    wait_time = between(0.5, 2.0)

    @task(4)
    def query_rag(self):
        """Simulates distributed scatter-gather RAG query (80% load)."""
        q = random.choice(SAMPLE_QUERIES)
        payload = {
            "query": q,
            "top_k": 5
        }
        with self.client.post("/api/v1/query", json=payload, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if "answer" in data and "telemetry" in data:
                    response.success()
                else:
                    response.failure("Response missing expected answer or telemetry fields")
            else:
                response.failure(f"HTTP {response.status_code}: {response.text}")

    @task(1)
    def ingest_document(self):
        """Simulates document ingestion with hash ring routing (20% load)."""
        doc_id, text = random.choice(SAMPLE_DOCS)
        payload = {
            "doc_id": f"{doc_id}_{random.randint(100, 999)}",
            "text": text,
            "replication_factor": 2
        }
        with self.client.post("/api/v1/documents/ingest", json=payload, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Ingest HTTP {response.status_code}: {response.text}")

import json
import logging
import asyncio
import time
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field
import grpc

from proto_gen import vector_service_pb2, vector_service_pb2_grpc
from shared.consistent_hash import ConsistentHashRing
from shared.document_processor import DocumentProcessor, DocumentChunk
from shared.embeddings import embeddings_engine
from shared.llm import llm_generator, RAGResponse
from shared.config import config
from shared.telemetry import (
    RAG_QUERY_LATENCY, RAG_CACHE_HITS, RAG_SHARD_LATENCY,
    RAG_NODE_HEALTH, RAG_DOCUMENTS_INGESTED, setup_telemetry
)
from coordinator.scatter_gather import ScatterGatherEngine
from coordinator.cache import QueryCache

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("CoordinatorService")

app = FastAPI(
    title="Distributed RAG Coordinator API",
    description="Coordinator microservice for sharded vector search, scatter-gather queries, caching, and fault tolerance.",
    version="2.5.0"
)

# Enable CORS for React Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics exporter route
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Initialize OpenTelemetry
setup_telemetry()


class CoordinatorState:
    """Maintains active gRPC channels, consistent hash ring, cache, and node health state."""

    def __init__(self):
        self.ring = ConsistentHashRing(virtual_nodes_per_node=config.VIRTUAL_NODES_PER_SHARD)
        self.doc_processor = DocumentProcessor(chunk_size=config.DEFAULT_CHUNK_SIZE, chunk_overlap=config.DEFAULT_CHUNK_OVERLAP)
        self.scatter_gather = ScatterGatherEngine(timeout_seconds=config.SHARD_TIMEOUT_SECONDS)
        self.cache = QueryCache(ttl_seconds=300)
        
        # Mapping: shard_id -> {address: "host:port", channel: gRPC_channel, stub: gRPC_stub, healthy: bool}
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def register_node(self, shard_id: str, host: str, port: int):
        """Registers a shard node endpoint, creates gRPC channel, and adds to consistent hash ring."""
        async with self._lock:
            address = f"{host}:{port}"
            channel = grpc.aio.insecure_channel(address)
            stub = vector_service_pb2_grpc.VectorStoreServiceStub(channel)
            
            self.nodes[shard_id] = {
                "shard_id": shard_id,
                "host": host,
                "port": port,
                "address": address,
                "channel": channel,
                "stub": stub,
                "healthy": True,
                "vector_count": 0,
                "last_check": time.time()
            }
            
            self.ring.add_node(shard_id)
            RAG_NODE_HEALTH.labels(shard_id=shard_id).set(1)
            logger.info(f"Registered shard node [{shard_id}] at {address}")

    async def get_active_stubs(self) -> Dict[str, vector_service_pb2_grpc.VectorStoreServiceStub]:
        """Returns dictionary of healthy shard stubs."""
        async with self._lock:
            return {
                shard_id: info["stub"]
                for shard_id, info in self.nodes.items()
                if info["healthy"]
            }

    async def check_node_health(self):
        """Background health check pinging gRPC HealthCheck endpoint on all registered nodes."""
        for shard_id, info in list(self.nodes.items()):
            stub = info["stub"]
            try:
                req = vector_service_pb2.HealthCheckRequest(client_id="coordinator_heartbeat")
                resp: vector_service_pb2.HealthCheckResponse = await asyncio.wait_for(
                    stub.HealthCheck(req),
                    timeout=1.0
                )
                if not info["healthy"]:
                    logger.info(f"HEARTBEAT: Shard node [{shard_id}] RECOVERED!")
                    info["healthy"] = True
                    self.ring.add_node(shard_id)
                info["vector_count"] = resp.vector_count
                RAG_NODE_HEALTH.labels(shard_id=shard_id).set(1)
            except Exception:
                if info["healthy"]:
                    logger.warning(f"HEARTBEAT: Shard node [{shard_id}] FAILED / UNREACHABLE! Removing from active hash ring.")
                    info["healthy"] = False
                    self.ring.remove_node(shard_id)
                RAG_NODE_HEALTH.labels(shard_id=shard_id).set(0)


coordinator = CoordinatorState()


@app.on_event("startup")
async def startup_event():
    """Initializes default local multi-node shard topology for local dev simulation."""
    default_shards = [
        ("shard_0", "localhost", 50051),
        ("shard_1", "localhost", 50052),
        ("shard_2", "localhost", 50053),
    ]
    for shard_id, host, port in default_shards:
        await coordinator.register_node(shard_id, host, port)

    asyncio.create_task(_heartbeat_loop())


async def _heartbeat_loop():
    while True:
        try:
            await coordinator.check_node_health()
        except Exception as e:
            logger.error(f"Heartbeat loop error: {e}")
        await asyncio.sleep(2.0)


# Request/Response Models
class IngestDocRequest(BaseModel):
    doc_id: str
    text: str
    metadata: Optional[Dict[str, Any]] = None
    replication_factor: int = 2


class IngestDocResponse(BaseModel):
    doc_id: str
    chunks_created: int
    target_shards: List[str]
    status: str


class DistributedQueryRequest(BaseModel):
    query: str
    top_k: int = 5


class DistributedQueryResponse(BaseModel):
    query: str
    answer: str
    sources: List[Dict[str, Any]]
    telemetry: Dict[str, Any]
    provider_used: str
    cached: bool = False


@app.post("/api/v1/documents/ingest", response_model=IngestDocResponse)
async def ingest_document(req: IngestDocRequest):
    """Ingests document text: chunks, embeds, routes to target primary + replica shards via consistent hashing."""
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Document text cannot be empty.")

    # Determine target shard nodes using Consistent Hash Ring
    target_shards = coordinator.ring.get_nodes(req.doc_id, replication_factor=req.replication_factor)
    if not target_shards:
        raise HTTPException(status_code=503, detail="No active shard nodes available in hash ring.")

    chunks = coordinator.doc_processor.process_document(req.doc_id, req.text, req.metadata)
    texts = [c.text for c in chunks]
    embeddings = embeddings_engine.embed_batch(texts)

    # Build gRPC chunks
    pb_chunks = []
    for i, c in enumerate(chunks):
        pb_chunks.append(vector_service_pb2.VectorChunk(
            chunk_id=c.chunk_id,
            doc_id=c.doc_id,
            text=c.text,
            chunk_index=c.chunk_index,
            metadata_json=json.dumps(c.metadata),
            embedding=embeddings[i].tolist()
        ))

    # Send gRPC IngestVector to primary and replica shards concurrently
    stubs = await coordinator.get_active_stubs()
    ingest_tasks = []
    
    for shard_id in target_shards:
        if shard_id in stubs:
            ingest_req = vector_service_pb2.IngestVectorRequest(
                shard_id=shard_id,
                chunks=pb_chunks
            )
            ingest_tasks.append(stubs[shard_id].IngestVector(ingest_req))

    if ingest_tasks:
        await asyncio.gather(*ingest_tasks, return_exceptions=True)

    # Invalidate query result cache on new document ingestion
    coordinator.cache.invalidate_all()
    RAG_DOCUMENTS_INGESTED.inc()

    return IngestDocResponse(
        doc_id=req.doc_id,
        chunks_created=len(chunks),
        target_shards=target_shards,
        status="success"
    )


@app.post("/api/v1/query", response_model=DistributedQueryResponse)
async def query_distributed_rag(req: DistributedQueryRequest):
    """Distributed scatter-gather query across shard nodes with caching, score merging, and LLM synthesis."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    start_time = time.perf_counter()

    # 1. Check Query Cache
    cached_res = coordinator.cache.get(req.query, req.top_k)
    if cached_res:
        RAG_CACHE_HITS.labels(status="hit").inc()
        cached_res["cached"] = True
        return DistributedQueryResponse(**cached_res)

    RAG_CACHE_HITS.labels(status="miss").inc()

    # 2. Embed user query text
    query_vec = embeddings_engine.embed_text(req.query)

    # 3. Parallel scatter-gather query across active vector shards
    active_stubs = await coordinator.get_active_stubs()
    merged_results, telemetry = await coordinator.scatter_gather.execute_scatter_gather(
        query_vector=query_vec,
        shard_stubs=active_stubs,
        top_k=req.top_k
    )

    # 4. Pass top retrieved chunks to LLM generator
    llm_resp: RAGResponse = llm_generator.generate_answer(req.query, merged_results)

    # 5. Format source chunks with shard attribution
    sources_formatted = [
        {
            "chunk_id": r.chunk_id,
            "doc_id": r.doc_id,
            "text": r.text,
            "score": round(r.score, 4),
            "shard_id": r.shard_id,
            "metadata": r.metadata
        }
        for r in llm_resp.sources
    ]

    total_latency_seconds = time.perf_counter() - start_time
    RAG_QUERY_LATENCY.observe(total_latency_seconds)

    response_dict = {
        "query": req.query,
        "answer": llm_resp.answer,
        "sources": sources_formatted,
        "telemetry": telemetry,
        "provider_used": llm_resp.provider_used,
        "cached": False
    }

    # Store in Redis Query Cache
    coordinator.cache.set(req.query, req.top_k, response_dict)

    return DistributedQueryResponse(**response_dict)


@app.get("/api/v1/cluster/status")
async def get_cluster_status():
    """Returns cluster nodes, health, ring key distribution %, and vector counts."""
    ring_dist = coordinator.ring.ring_distribution()
    nodes_summary = []
    
    for shard_id, info in coordinator.nodes.items():
        nodes_summary.append({
            "shard_id": shard_id,
            "address": info["address"],
            "healthy": info["healthy"],
            "vector_count": info["vector_count"],
            "ring_share_pct": ring_dist.get(shard_id, 0.0)
        })

    return {
        "total_nodes": len(coordinator.nodes),
        "healthy_nodes": sum(1 for n in coordinator.nodes.values() if n["healthy"]),
        "ring_distribution": ring_dist,
        "nodes": nodes_summary
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

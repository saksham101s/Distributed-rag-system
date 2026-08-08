import os
import sys

# Ensure project root is in sys.path for direct script execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import json
import logging
import argparse
import asyncio
import grpc

from proto_gen import vector_service_pb2, vector_service_pb2_grpc

from shared.faiss_store import FAISSVectorStore, SearchResult
from shared.document_processor import DocumentChunk
from shared.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ShardNodeService")


class VectorStoreServicer(vector_service_pb2_grpc.VectorStoreServiceServicer):
    """gRPC Servicer implementing vector storage and similarity search for a single shard."""

    def __init__(self, shard_id: str):
        self.shard_id = shard_id
        self.vector_store = FAISSVectorStore(dim=config.EMBEDDING_DIM, shard_id=shard_id)
        self.start_time = time.time()

    async def IngestVector(self, request: vector_service_pb2.IngestVectorRequest, context) -> vector_service_pb2.IngestVectorResponse:
        """Ingests chunks into shard's local FAISS index."""
        start = time.perf_counter()
        try:
            chunks = []
            embeddings_list = []
            
            for item in request.chunks:
                meta = json.loads(item.metadata_json) if item.metadata_json else {}
                chunk = DocumentChunk(
                    chunk_id=item.chunk_id,
                    doc_id=item.doc_id,
                    text=item.text,
                    chunk_index=item.chunk_index,
                    metadata=meta
                )
                chunks.append(chunk)
                embeddings_list.append(list(item.embedding))

            if chunks:
                import numpy as np
                vec_np = np.array(embeddings_list, dtype=np.float32)
                self.vector_store.add_chunks(chunks, vec_np)

            elapsed_ms = (time.perf_counter() - start) * 1000.0
            logger.info(f"[{self.shard_id}] Ingested {len(chunks)} chunks in {elapsed_ms:.2f}ms (Total count: {self.vector_store.count()})")
            
            return vector_service_pb2.IngestVectorResponse(
                success=True,
                inserted_count=len(chunks),
                message=f"[{self.shard_id}] Successfully ingested {len(chunks)} vectors."
            )
        except Exception as e:
            logger.error(f"[{self.shard_id}] Ingest error: {e}")
            return vector_service_pb2.IngestVectorResponse(
                success=False,
                inserted_count=0,
                message=str(e)
            )

    async def SearchVector(self, request: vector_service_pb2.SearchRequest, context) -> vector_service_pb2.SearchResponse:
        """Executes similarity vector search on local FAISS index."""
        start = time.perf_counter()
        try:
            import numpy as np
            query_vec = np.array(request.query_vector, dtype=np.float32)
            top_k = request.top_k or 5

            results: List[SearchResult] = self.vector_store.search(query_vec, top_k=top_k)
            
            pb_items = []
            for r in results:
                pb_items.append(vector_service_pb2.SearchResultItem(
                    chunk_id=r.chunk_id,
                    doc_id=r.doc_id,
                    text=r.text,
                    score=r.score,
                    chunk_index=r.chunk_index,
                    metadata_json=json.dumps(r.metadata),
                    shard_id=self.shard_id
                ))

            elapsed_ms = (time.perf_counter() - start) * 1000.0
            logger.info(f"[{self.shard_id}] Vector search completed in {elapsed_ms:.2f}ms. Returned {len(pb_items)} items.")
            
            return vector_service_pb2.SearchResponse(
                results=pb_items,
                shard_id=self.shard_id,
                latency_ms=elapsed_ms
            )
        except Exception as e:
            logger.error(f"[{self.shard_id}] Search error: {e}")
            return vector_service_pb2.SearchResponse(
                results=[],
                shard_id=self.shard_id,
                latency_ms=0.0
            )

    async def HealthCheck(self, request: vector_service_pb2.HealthCheckRequest, context) -> vector_service_pb2.HealthCheckResponse:
        """Returns node health, vector count, and uptime."""
        uptime = time.time() - self.start_time
        return vector_service_pb2.HealthCheckResponse(
            shard_id=self.shard_id,
            healthy=True,
            vector_count=self.vector_store.count(),
            uptime_seconds=uptime
        )


async def serve(shard_id: str, port: int):
    """Starts gRPC AsyncIO server for the shard node."""
    server = grpc.aio.server()
    servicer = VectorStoreServicer(shard_id=shard_id)
    vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicer, server)
    
    listen_addr = f"0.0.0.0:{port}"
    server.add_insecure_port(listen_addr)
    logger.info(f"Starting gRPC Shard Node [{shard_id}] listening on {listen_addr}...")
    
    await server.start()
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info(f"Shutting down gRPC Shard Node [{shard_id}]...")
        await server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed RAG gRPC Shard Node")
    parser.add_argument("--shard-id", type=str, default="shard_0", help="Unique identifier for this shard node")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen for gRPC connections")
    args = parser.parse_args()

    asyncio.run(serve(shard_id=args.shard_id, port=args.port))

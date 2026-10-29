import os
import sys

# Ensure project root is in sys.path for direct script execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import json
import logging
import argparse
import asyncio
from typing import Dict, List

import grpc
import numpy as np

from proto_gen import vector_service_pb2, vector_service_pb2_grpc

from shared.faiss_store import FAISSVectorStore, SearchResult
from shared.document_processor import DocumentChunk
from shared.config import config
from shared.telemetry import setup_telemetry, instrument_grpc_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ShardNodeService")


class VectorStoreServicer(vector_service_pb2_grpc.VectorStoreServiceServicer):
    """gRPC Servicer implementing vector storage and similarity search for a single shard."""

    def __init__(self, shard_id: str, chaos_enabled: bool = config.CHAOS_ENABLED):
        self.shard_id = shard_id
        self.vector_store = FAISSVectorStore(dim=config.EMBEDDING_DIM, shard_id=shard_id)
        self.start_time = time.time()
        self.chaos_enabled = chaos_enabled
        self.fault_mode = "none"
        self.fault_latency_ms = 0

    async def _fault_gate(self, context):
        """Applies the injected fault (if any) before serving an RPC."""
        if self.fault_mode == "down":
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"[{self.shard_id}] injected fault: down")
        elif self.fault_mode == "slow" and self.fault_latency_ms > 0:
            await asyncio.sleep(self.fault_latency_ms / 1000.0)

    async def IngestVector(self, request: vector_service_pb2.IngestVectorRequest, context) -> vector_service_pb2.IngestVectorResponse:
        """Upserts chunks into the local FAISS index. Each doc_id in the request replaces any older copy of that doc."""
        await self._fault_gate(context)
        start = time.perf_counter()
        try:
            by_doc: Dict[str, List[vector_service_pb2.VectorChunk]] = {}
            for item in request.chunks:
                if len(item.embedding) != config.EMBEDDING_DIM:
                    raise ValueError(
                        f"chunk {item.chunk_id}: embedding has {len(item.embedding)} dims, expected {config.EMBEDDING_DIM}"
                    )
                by_doc.setdefault(item.doc_id, []).append(item)

            inserted, skipped = 0, []
            for doc_id, items in by_doc.items():
                chunks = [
                    DocumentChunk(
                        chunk_id=item.chunk_id,
                        doc_id=item.doc_id,
                        text=item.text,
                        chunk_index=item.chunk_index,
                        metadata=json.loads(item.metadata_json) if item.metadata_json else {},
                    )
                    for item in items
                ]
                vecs = np.array([list(item.embedding) for item in items], dtype=np.float32)
                applied = self.vector_store.upsert_document(
                    doc_id, chunks, vecs,
                    version=items[0].doc_version,
                    replication_factor=items[0].replication_factor,
                )
                if applied:
                    inserted += len(chunks)
                else:
                    skipped.append(doc_id)

            elapsed_ms = (time.perf_counter() - start) * 1000.0
            source = "repair" if request.is_replica_sync else "ingest"
            logger.info(
                f"[{self.shard_id}] {source}: upserted {inserted} chunks in {elapsed_ms:.2f}ms "
                f"(stale skipped: {skipped or 'none'}, total vectors: {self.vector_store.count()})"
            )
            message = f"[{self.shard_id}] Upserted {inserted} vectors."
            if skipped:
                message += f" Skipped stale writes for: {', '.join(skipped)}."
            return vector_service_pb2.IngestVectorResponse(success=True, inserted_count=inserted, message=message)
        except Exception as e:
            logger.error(f"[{self.shard_id}] Ingest error: {e}")
            return vector_service_pb2.IngestVectorResponse(success=False, inserted_count=0, message=str(e))

    async def SearchVector(self, request: vector_service_pb2.SearchRequest, context) -> vector_service_pb2.SearchResponse:
        """Executes similarity vector search on local FAISS index."""
        await self._fault_gate(context)
        start = time.perf_counter()
        if len(request.query_vector) != config.EMBEDDING_DIM:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"query_vector has {len(request.query_vector)} dims, expected {config.EMBEDDING_DIM}",
            )
        try:
            query_vec = np.array(request.query_vector, dtype=np.float32)
            results: List[SearchResult] = self.vector_store.search(query_vec, top_k=request.top_k or config.DEFAULT_TOP_K)

            pb_items = [
                vector_service_pb2.SearchResultItem(
                    chunk_id=r.chunk_id,
                    doc_id=r.doc_id,
                    text=r.text,
                    score=r.score,
                    chunk_index=r.chunk_index,
                    metadata_json=json.dumps(r.metadata),
                    shard_id=self.shard_id,
                    doc_version=r.doc_version,
                )
                for r in results
            ]

            elapsed_ms = (time.perf_counter() - start) * 1000.0
            logger.info(f"[{self.shard_id}] Vector search completed in {elapsed_ms:.2f}ms. Returned {len(pb_items)} items.")
            return vector_service_pb2.SearchResponse(results=pb_items, shard_id=self.shard_id, latency_ms=elapsed_ms)
        except Exception as e:
            logger.error(f"[{self.shard_id}] Search error: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, f"search failed: {e}")

    async def HealthCheck(self, request: vector_service_pb2.HealthCheckRequest, context) -> vector_service_pb2.HealthCheckResponse:
        """Returns node health, vector count, and uptime."""
        await self._fault_gate(context)
        return vector_service_pb2.HealthCheckResponse(
            shard_id=self.shard_id,
            healthy=True,
            vector_count=self.vector_store.count(),
            uptime_seconds=time.time() - self.start_time,
        )

    async def DeleteDocument(self, request: vector_service_pb2.DeleteDocumentRequest, context) -> vector_service_pb2.DeleteDocumentResponse:
        """Removes every chunk of a document (optionally only if older than `below_version`)."""
        await self._fault_gate(context)
        deleted = self.vector_store.delete_by_doc_id(request.doc_id, below_version=request.below_version)
        return vector_service_pb2.DeleteDocumentResponse(deleted_count=deleted)

    async def ListDocuments(self, request: vector_service_pb2.ListDocumentsRequest, context) -> vector_service_pb2.ListDocumentsResponse:
        """Lists the documents held by this shard, with versions, for the coordinator's repair loop."""
        await self._fault_gate(context)
        return vector_service_pb2.ListDocumentsResponse(documents=[
            vector_service_pb2.DocumentSummary(
                doc_id=info.doc_id,
                doc_version=info.version,
                chunk_count=info.chunk_count,
                replication_factor=info.replication_factor,
                metadata_json=json.dumps(info.metadata),
            )
            for info in self.vector_store.list_documents()
        ])

    async def FetchDocument(self, request: vector_service_pb2.FetchDocumentRequest, context) -> vector_service_pb2.FetchDocumentResponse:
        """Returns a document's chunks with their embeddings so it can be copied to another shard."""
        await self._fault_gate(context)
        info, chunks, vecs = self.vector_store.get_document(request.doc_id)
        if info is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"doc '{request.doc_id}' not on {self.shard_id}")
        return vector_service_pb2.FetchDocumentResponse(chunks=[
            vector_service_pb2.VectorChunk(
                chunk_id=c.chunk_id,
                doc_id=c.doc_id,
                text=c.text,
                chunk_index=c.chunk_index,
                metadata_json=json.dumps(c.metadata),
                embedding=[] if request.omit_embeddings else vecs[i].tolist(),
                doc_version=info.version,
                replication_factor=info.replication_factor,
            )
            for i, c in enumerate(chunks)
        ])

    async def SetFault(self, request: vector_service_pb2.FaultRequest, context) -> vector_service_pb2.FaultResponse:
        """Dev-only fault injection: take the shard down, slow it, or wipe its in-memory index (simulated crash-restart)."""
        if not self.chaos_enabled:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "fault injection disabled (set CHAOS_ENABLED=true)")
        mode = request.mode or "none"
        if mode not in ("none", "down", "slow", "wipe"):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"unknown fault mode '{mode}'")
        if mode == "wipe":
            self.vector_store.clear()
            self.start_time = time.time()
        else:
            self.fault_mode = mode
            self.fault_latency_ms = max(0, request.latency_ms) if mode == "slow" else 0
        logger.warning(f"[{self.shard_id}] CHAOS: fault mode={self.fault_mode} latency_ms={self.fault_latency_ms} (request: {mode})")
        return vector_service_pb2.FaultResponse(
            mode=self.fault_mode, latency_ms=self.fault_latency_ms, vector_count=self.vector_store.count()
        )


async def serve(shard_id: str, port: int):
    """Starts gRPC AsyncIO server for the shard node."""
    setup_telemetry(config.TRACING_EXPORTER)
    instrument_grpc_server()
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

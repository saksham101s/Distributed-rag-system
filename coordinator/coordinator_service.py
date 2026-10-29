import os
import sys

# Ensure project root is in sys.path for direct script execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging
import asyncio
import threading
import time
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Literal, Optional, Tuple
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field
import grpc

from proto_gen import vector_service_pb2, vector_service_pb2_grpc

from shared.consistent_hash import ConsistentHashRing
from shared.document_processor import DocumentProcessor
from shared.embeddings import embeddings_engine
from shared.llm import llm_generator, RAGResponse, StreamEvent
from shared.faiss_store import SearchResult
from shared.text_extraction import UnsupportedFileType, doc_id_from_filename, extract_text
from shared.config import config
from shared.telemetry import (
    RAG_QUERY_LATENCY, RAG_CACHE_HITS, RAG_NODE_HEALTH, RAG_DOCUMENTS_INGESTED, RAG_REPAIR_COPIES,
    setup_telemetry, instrument_fastapi, instrument_grpc_client, span,
)
from coordinator.scatter_gather import ScatterGatherEngine
from coordinator.cache import QueryCache
from coordinator.events import EventBus, sse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("CoordinatorService")

# Writes and repair copies move whole documents, so they get a longer deadline than searches
WRITE_TIMEOUT_SECONDS = max(5.0, config.SHARD_TIMEOUT_SECONDS)


class IngestResult(BaseModel):
    """Outcome of writing one document to its target shards."""
    doc_id: str
    doc_version: int
    chunks_created: int
    target_shards: List[str]
    succeeded_shards: List[str]
    failed_shards: Dict[str, str]


class CoordinatorState:
    """Maintains gRPC channels, the consistent hash ring, the cache, node health and replica repair."""

    def __init__(self):
        self.ring = ConsistentHashRing(virtual_nodes_per_node=config.VIRTUAL_NODES_PER_SHARD)
        self.doc_processor = DocumentProcessor(chunk_size=config.DEFAULT_CHUNK_SIZE, chunk_overlap=config.DEFAULT_CHUNK_OVERLAP)
        self.scatter_gather = ScatterGatherEngine(timeout_seconds=config.SHARD_TIMEOUT_SECONDS)
        self.cache = QueryCache()

        # Mapping: shard_id -> {address, channel, stub, healthy, vector_count, last_check}
        self.nodes: Dict[str, Dict[str, Any]] = {}
        # doc_id -> version at which it was deleted, so repair doesn't resurrect it from a shard that missed the delete
        self.tombstones: Dict[str, int] = {}
        self.last_repair: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._repair_lock = asyncio.Lock()
        self.repair_requested = asyncio.Event()
        self.events = EventBus()
        # shard_id -> last injected fault {"mode", "latency_ms"} (dev-only chaos endpoints)
        self.faults: Dict[str, Dict[str, Any]] = {}

    async def register_node(self, shard_id: str, host: str, port: int):
        """Registers a shard node endpoint, creates its gRPC channel and adds it to the hash ring."""
        async with self._lock:
            address = f"{host}:{port}"
            # Cap reconnect backoff so a restarted shard is noticed within a few heartbeats
            channel = grpc.aio.insecure_channel(address, options=[
                ("grpc.initial_reconnect_backoff_ms", 200),
                ("grpc.min_reconnect_backoff_ms", 200),
                ("grpc.max_reconnect_backoff_ms", 2000),
            ])
            self.nodes[shard_id] = {
                "shard_id": shard_id,
                "host": host,
                "port": port,
                "address": address,
                "channel": channel,
                "stub": vector_service_pb2_grpc.VectorStoreServiceStub(channel),
                "healthy": True,
                "vector_count": 0,
                "last_check": time.time(),
            }
            self.ring.add_node(shard_id)
            RAG_NODE_HEALTH.labels(shard_id=shard_id).set(1)
            logger.info(f"Registered shard node [{shard_id}] at {address}")

    async def close(self):
        """Closes every gRPC channel."""
        for info in self.nodes.values():
            await info["channel"].close()

    async def get_active_stubs(self) -> Dict[str, vector_service_pb2_grpc.VectorStoreServiceStub]:
        """Returns dictionary of healthy shard stubs."""
        async with self._lock:
            return {shard_id: info["stub"] for shard_id, info in self.nodes.items() if info["healthy"]}

    async def check_node_health(self) -> bool:
        """Pings every node's HealthCheck concurrently and updates the ring. Returns True if membership changed."""
        async def probe(stub) -> Optional[vector_service_pb2.HealthCheckResponse]:
            try:
                req = vector_service_pb2.HealthCheckRequest(client_id="coordinator_heartbeat")
                return await asyncio.wait_for(stub.HealthCheck(req), timeout=1.0)
            except Exception:
                return None

        snapshot = list(self.nodes.items())
        responses = await asyncio.gather(*(probe(info["stub"]) for _, info in snapshot))

        changed = False
        async with self._lock:
            for (shard_id, info), resp in zip(snapshot, responses):
                info["last_check"] = time.time()
                if resp is not None and resp.healthy:
                    if not info["healthy"]:
                        logger.info(f"HEARTBEAT: Shard node [{shard_id}] RECOVERED! Re-adding to hash ring.")
                        info["healthy"] = True
                        self.ring.add_node(shard_id)
                        changed = True
                        self.events.publish("node_up", {"shard_id": shard_id, "address": info["address"]})
                    info["vector_count"] = resp.vector_count
                    RAG_NODE_HEALTH.labels(shard_id=shard_id).set(1)
                else:
                    if info["healthy"]:
                        logger.warning(f"HEARTBEAT: Shard node [{shard_id}] FAILED / UNREACHABLE! Removing from active hash ring.")
                        info["healthy"] = False
                        self.ring.remove_node(shard_id)
                        changed = True
                        self.events.publish("node_down", {"shard_id": shard_id, "address": info["address"]})
                    RAG_NODE_HEALTH.labels(shard_id=shard_id).set(0)

        if changed:
            self.repair_requested.set()
        return changed

    async def _write_to_shards(
        self,
        stubs: Dict[str, vector_service_pb2_grpc.VectorStoreServiceStub],
        shard_ids: List[str],
        pb_chunks: List[vector_service_pb2.VectorChunk],
        is_replica_sync: bool,
    ) -> Tuple[List[str], Dict[str, str]]:
        """Sends one IngestVector to each shard concurrently. Returns (succeeded, {failed_shard: reason})."""
        async def write(shard_id: str) -> Optional[str]:
            stub = stubs.get(shard_id)
            if stub is None:
                return "shard not healthy"
            try:
                resp = await asyncio.wait_for(
                    stub.IngestVector(vector_service_pb2.IngestVectorRequest(
                        shard_id=shard_id, chunks=pb_chunks, is_replica_sync=is_replica_sync,
                    )),
                    timeout=WRITE_TIMEOUT_SECONDS,
                )
                return None if resp.success else (resp.message or "shard rejected write")
            except asyncio.TimeoutError:
                return "timeout"
            except Exception as e:
                return str(e)

        errors = await asyncio.gather(*(write(s) for s in shard_ids))
        succeeded = [s for s, err in zip(shard_ids, errors) if err is None]
        failed = {s: err for s, err in zip(shard_ids, errors) if err is not None}
        return succeeded, failed

    async def ingest(
        self,
        doc_id: str,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
        replication_factor: Optional[int] = None,
    ) -> IngestResult:
        """Chunks, embeds and upserts a document on its ring-assigned shards, then drops older copies elsewhere."""
        rf = replication_factor or config.DEFAULT_REPLICATION_FACTOR
        version = time.time_ns()
        target_shards = self.ring.get_nodes(doc_id, replication_factor=rf)
        if not target_shards:
            raise HTTPException(status_code=503, detail="No active shard nodes available in hash ring.")

        chunks = self.doc_processor.process_document(doc_id, text, metadata)
        with span("embed_batch", chunks=len(chunks)):
            embeddings = await asyncio.to_thread(embeddings_engine.embed_batch, [c.text for c in chunks])

        pb_chunks = [
            vector_service_pb2.VectorChunk(
                chunk_id=c.chunk_id,
                doc_id=c.doc_id,
                text=c.text,
                chunk_index=c.chunk_index,
                metadata_json=json.dumps(c.metadata),
                embedding=embeddings[i].tolist(),
                doc_version=version,
                replication_factor=rf,
            )
            for i, c in enumerate(chunks)
        ]

        stubs = await self.get_active_stubs()
        succeeded, failed = await self._write_to_shards(stubs, target_shards, pb_chunks, is_replica_sync=False)

        if succeeded:
            # The doc may have lived on other shards before (ring changed, or a different replication factor)
            others = [s for s in stubs if s not in target_shards]
            await asyncio.gather(
                *(self._delete_on_shard(stubs[s], doc_id, below_version=version) for s in others),
                return_exceptions=True,
            )
            self.tombstones.pop(doc_id, None)
            await asyncio.to_thread(self.cache.invalidate_all)
            RAG_DOCUMENTS_INGESTED.inc()
            self.events.publish("document_ingested", {
                "doc_id": doc_id, "doc_version": version, "chunks": len(chunks),
                "target_shards": target_shards, "failed_shards": failed,
            })
        if failed:
            # Any shard that missed the write gets filled in by the repair loop
            self.repair_requested.set()

        return IngestResult(
            doc_id=doc_id,
            doc_version=version,
            chunks_created=len(chunks),
            target_shards=target_shards,
            succeeded_shards=succeeded,
            failed_shards=failed,
        )

    async def _delete_on_shard(self, stub, doc_id: str, below_version: int = 0) -> int:
        resp = await asyncio.wait_for(
            stub.DeleteDocument(vector_service_pb2.DeleteDocumentRequest(doc_id=doc_id, below_version=below_version)),
            timeout=WRITE_TIMEOUT_SECONDS,
        )
        return resp.deleted_count

    async def delete(self, doc_id: str) -> Dict[str, Any]:
        """Deletes a document from every healthy shard and records a tombstone for shards that are down."""
        self.tombstones[doc_id] = time.time_ns()
        stubs = await self.get_active_stubs()
        results = await asyncio.gather(
            *(self._delete_on_shard(stub, doc_id) for stub in stubs.values()), return_exceptions=True
        )
        deleted = {s: r for s, r in zip(stubs, results) if not isinstance(r, Exception)}
        failed = {s: str(r) for s, r in zip(stubs, results) if isinstance(r, Exception)}
        await asyncio.to_thread(self.cache.invalidate_all)
        result = {"doc_id": doc_id, "deleted_chunks": deleted, "failed_shards": failed}
        self.events.publish("document_deleted", result)
        return result

    async def repair(self) -> Dict[str, Any]:
        """Anti-entropy pass: makes every doc live (at its latest version) on exactly its ring-assigned shards.

        Copies missing/stale replicas from a shard that holds the latest version, then trims copies on shards
        the ring no longer assigns. Only healthy shards take part.
        """
        async with self._repair_lock:
            start = time.perf_counter()
            stubs = await self.get_active_stubs()
            summary: Dict[str, Any] = {"copied": 0, "trimmed": 0, "tombstoned": 0, "errors": []}
            holders, list_errors = await self._collect_listings()
            summary["errors"].extend(f"list: {e}" for e in list_errors)

            for doc_id, held in holders.items():
                try:
                    latest = max(d.doc_version for d in held.values())
                    tombstone = self.tombstones.get(doc_id)
                    if tombstone is not None and latest < tombstone:
                        for shard_id in held:
                            await self._delete_on_shard(stubs[shard_id], doc_id, below_version=tombstone)
                        summary["tombstoned"] += 1
                        continue

                    fresh = {s for s, d in held.items() if d.doc_version == latest}
                    rf = next(d.replication_factor for d in held.values() if d.doc_version == latest) \
                        or config.DEFAULT_REPLICATION_FACTOR
                    targets = self.ring.get_nodes(doc_id, replication_factor=rf)
                    missing = [t for t in targets if t not in fresh]

                    if missing:
                        source = next((s for s in targets if s in fresh), next(iter(fresh)))
                        fetched = await asyncio.wait_for(
                            stubs[source].FetchDocument(vector_service_pb2.FetchDocumentRequest(doc_id=doc_id)),
                            timeout=WRITE_TIMEOUT_SECONDS,
                        )
                        copied, failed = await self._write_to_shards(stubs, missing, list(fetched.chunks), is_replica_sync=True)
                        fresh.update(copied)
                        summary["copied"] += len(copied)
                        RAG_REPAIR_COPIES.labels(action="copied").inc(len(copied))
                        summary["errors"].extend(f"{doc_id} -> {s}: {e}" for s, e in failed.items())

                    if all(t in fresh for t in targets):
                        extras = [s for s in held if s not in targets]
                        for shard_id in extras:
                            # below_version guards against deleting a newer copy written while we were working
                            await self._delete_on_shard(stubs[shard_id], doc_id, below_version=latest + 1)
                        summary["trimmed"] += len(extras)
                        RAG_REPAIR_COPIES.labels(action="trimmed").inc(len(extras))
                except Exception as e:
                    summary["errors"].append(f"{doc_id}: {e}")

            if summary["copied"] or summary["trimmed"] or summary["tombstoned"]:
                await asyncio.to_thread(self.cache.invalidate_all)
            summary.update({
                "documents_seen": len(holders),
                "shards": sorted(stubs),
                "duration_ms": round((time.perf_counter() - start) * 1000.0, 2),
                "finished_at": time.time(),
            })
            self.last_repair = summary
            if summary["copied"] or summary["trimmed"] or summary["tombstoned"] or summary["errors"]:
                logger.info(f"REPAIR: {summary}")
                self.events.publish("repair_completed", summary)
            return summary

    def status_snapshot(self) -> Dict[str, Any]:
        """Cluster topology, health and ring share, as served by /cluster/status and the event stream."""
        ring_dist = self.ring.ring_distribution()
        return {
            "total_nodes": len(self.nodes),
            "healthy_nodes": sum(1 for n in self.nodes.values() if n["healthy"]),
            "replication_factor": config.DEFAULT_REPLICATION_FACTOR,
            "ring_distribution": ring_dist,
            "nodes": [
                {
                    "shard_id": shard_id,
                    "address": info["address"],
                    "healthy": info["healthy"],
                    "vector_count": info["vector_count"],
                    "ring_share_pct": ring_dist.get(shard_id, 0.0),
                    "fault": self.faults.get(shard_id, {"mode": "none", "latency_ms": 0}),
                }
                for shard_id, info in self.nodes.items()
            ],
            "last_repair": self.last_repair,
            "chaos_enabled": config.CHAOS_ENABLED,
        }

    async def _collect_listings(self) -> Tuple[Dict[str, Dict[str, vector_service_pb2.DocumentSummary]], List[str]]:
        """ListDocuments on every healthy shard → ({doc_id: {shard_id: summary}}, errors)."""
        stubs = await self.get_active_stubs()

        async def list_docs(shard_id: str):
            resp = await asyncio.wait_for(
                stubs[shard_id].ListDocuments(vector_service_pb2.ListDocumentsRequest()),
                timeout=config.SHARD_TIMEOUT_SECONDS,
            )
            return shard_id, resp.documents

        holders: Dict[str, Dict[str, vector_service_pb2.DocumentSummary]] = {}
        errors: List[str] = []
        for item in await asyncio.gather(*(list_docs(s) for s in stubs), return_exceptions=True):
            if isinstance(item, Exception):
                errors.append(str(item))
                continue
            shard_id, docs = item
            for d in docs:
                holders.setdefault(d.doc_id, {})[shard_id] = d
        return holders, errors

    def _describe_document(self, doc_id: str, held: Dict[str, vector_service_pb2.DocumentSummary]) -> Dict[str, Any]:
        """Merges per-shard summaries of one doc into a catalog entry with its replication health."""
        latest = max(held.values(), key=lambda d: d.doc_version)
        rf = latest.replication_factor or config.DEFAULT_REPLICATION_FACTOR
        targets = self.ring.get_nodes(doc_id, replication_factor=rf)
        fresh = {s for s, d in held.items() if d.doc_version == latest.doc_version}
        if any(t not in fresh for t in targets):
            status = "under_replicated"
        elif any(s not in targets for s in held):
            status = "over_replicated"
        else:
            status = "ok"
        return {
            "doc_id": doc_id,
            "doc_version": latest.doc_version,
            "ingested_at": latest.doc_version / 1e9,
            "chunk_count": latest.chunk_count,
            "replication_factor": rf,
            "metadata": json.loads(latest.metadata_json) if latest.metadata_json else {},
            "target_shards": targets,
            "replicas": [
                {"shard_id": s, "doc_version": d.doc_version, "stale": d.doc_version != latest.doc_version}
                for s, d in sorted(held.items())
            ],
            "replication_status": status,
        }

    async def list_documents(self) -> Dict[str, Any]:
        """Catalog of every document on the healthy shards, newest first."""
        holders, errors = await self._collect_listings()
        docs = [self._describe_document(doc_id, held) for doc_id, held in holders.items()]
        docs.sort(key=lambda d: d["doc_version"], reverse=True)
        return {"documents": docs, "errors": errors}

    async def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Catalog entry plus chunk texts for one document, or None if no healthy shard holds it."""
        holders, _ = await self._collect_listings()
        held = holders.get(doc_id)
        if not held:
            return None
        entry = self._describe_document(doc_id, held)
        stubs = await self.get_active_stubs()
        source = next(r["shard_id"] for r in entry["replicas"] if not r["stale"])
        fetched = await asyncio.wait_for(
            stubs[source].FetchDocument(vector_service_pb2.FetchDocumentRequest(doc_id=doc_id, omit_embeddings=True)),
            timeout=WRITE_TIMEOUT_SECONDS,
        )
        entry["chunks"] = [
            {"chunk_id": c.chunk_id, "chunk_index": c.chunk_index, "text": c.text}
            for c in sorted(fetched.chunks, key=lambda c: c.chunk_index)
        ]
        entry["served_by"] = source
        return entry

    async def set_fault(self, shard_id: str, mode: str, latency_ms: int = 0) -> Dict[str, Any]:
        """Dev-only: injects a fault on one shard (down / slow / wipe / none) and lets heartbeat + repair react."""
        info = self.nodes.get(shard_id)
        if info is None:
            raise HTTPException(status_code=404, detail=f"Unknown shard '{shard_id}'")
        try:
            resp = await asyncio.wait_for(
                info["stub"].SetFault(vector_service_pb2.FaultRequest(mode=mode, latency_ms=latency_ms)),
                timeout=WRITE_TIMEOUT_SECONDS,
            )
        except grpc.aio.AioRpcError as e:
            raise HTTPException(status_code=502, detail=f"{shard_id}: {e.details()}")
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail=f"{shard_id}: SetFault timed out")

        self.faults[shard_id] = {"mode": resp.mode, "latency_ms": resp.latency_ms}
        result = {"shard_id": shard_id, "requested": mode, "mode": resp.mode,
                  "latency_ms": resp.latency_ms, "vector_count": resp.vector_count}
        self.events.publish("fault_injected", result)
        # Down/up shows up at the next heartbeat; a wipe keeps the shard healthy, so ask for repair directly
        await self.check_node_health()
        if mode == "wipe":
            self.repair_requested.set()
        return result


coordinator = CoordinatorState()


async def _heartbeat_loop():
    while True:
        try:
            await coordinator.check_node_health()
            if coordinator.events.subscriber_count:
                coordinator.events.publish("cluster_status", coordinator.status_snapshot())
        except Exception as e:
            logger.error(f"Heartbeat loop error: {e}")
        await asyncio.sleep(config.HEARTBEAT_INTERVAL_SECONDS)


async def _repair_loop():
    interval = config.REPAIR_INTERVAL_SECONDS or None
    while True:
        try:
            await asyncio.wait_for(coordinator.repair_requested.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        coordinator.repair_requested.clear()
        try:
            await coordinator.repair()
        except Exception as e:
            logger.error(f"Repair loop error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Registers the shard topology from SHARD_NODES and runs the heartbeat + repair loops."""
    for shard_id, host, port in config.shard_nodes():
        await coordinator.register_node(shard_id, host, port)
    tasks = [asyncio.create_task(_heartbeat_loop()), asyncio.create_task(_repair_loop())]
    yield
    for t in tasks:
        t.cancel()
    await coordinator.close()


setup_telemetry(config.TRACING_EXPORTER)
instrument_grpc_client()

app = FastAPI(
    title="Distributed RAG Coordinator API",
    description="Coordinator microservice for sharded vector search, scatter-gather queries, caching, and fault tolerance.",
    version="2.6.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics exporter route
app.mount("/metrics", make_asgi_app())
instrument_fastapi(app)


# Request/Response Models
class IngestDocRequest(BaseModel):
    doc_id: str = Field(min_length=1, max_length=256)
    text: str
    metadata: Optional[Dict[str, Any]] = None
    replication_factor: Optional[int] = Field(default=None, ge=1, le=16)


class IngestDocResponse(BaseModel):
    doc_id: str
    doc_version: int
    chunks_created: int
    target_shards: List[str]
    failed_shards: Dict[str, str]
    status: str  # "success" | "partial"
    characters: Optional[int] = None  # set for file uploads: length of the extracted text


class DistributedQueryRequest(BaseModel):
    query: str
    top_k: int = Field(default=config.DEFAULT_TOP_K, ge=1, le=50)


class DistributedQueryResponse(BaseModel):
    query: str
    answer: str
    sources: List[Dict[str, Any]]
    telemetry: Dict[str, Any]
    provider_used: str
    cached: bool = False


class FaultInjectionRequest(BaseModel):
    mode: Literal["none", "down", "slow", "wipe"]
    latency_ms: int = Field(default=0, ge=0, le=30000)


def _ingest_response(result: IngestResult, characters: Optional[int] = None) -> IngestDocResponse:
    if not result.succeeded_shards:
        raise HTTPException(status_code=502, detail={"message": "No target shard accepted the write.", "failed_shards": result.failed_shards})
    return IngestDocResponse(
        doc_id=result.doc_id,
        doc_version=result.doc_version,
        chunks_created=result.chunks_created,
        target_shards=result.target_shards,
        failed_shards=result.failed_shards,
        status="partial" if result.failed_shards else "success",
        characters=characters,
    )


def _format_sources(results: List[SearchResult]) -> List[Dict[str, Any]]:
    return [
        {
            "chunk_id": r.chunk_id,
            "doc_id": r.doc_id,
            "text": r.text,
            "score": round(r.score, 4),
            "shard_id": r.shard_id,
            "metadata": r.metadata
        }
        for r in results
    ]


async def _retrieve(query: str, top_k: int) -> Tuple[List[SearchResult], Dict[str, Any]]:
    """Embeds the query (off the event loop) and scatter-gathers it across the healthy shards."""
    with span("embed_query"):
        query_vec = await asyncio.to_thread(embeddings_engine.embed_text, query)
    active_stubs = await coordinator.get_active_stubs()
    return await coordinator.scatter_gather.execute_scatter_gather(
        query_vector=query_vec, shard_stubs=active_stubs, top_k=top_k
    )


def _require_chaos():
    if not config.CHAOS_ENABLED:
        raise HTTPException(status_code=403, detail="Fault injection is disabled. Start the coordinator and shards with CHAOS_ENABLED=true.")


@app.post("/api/v1/documents/ingest", response_model=IngestDocResponse)
async def ingest_document(req: IngestDocRequest):
    """Chunks, embeds and upserts a document onto its primary + replica shards (consistent hashing)."""
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Document text cannot be empty.")
    result = await coordinator.ingest(req.doc_id, req.text, req.metadata, req.replication_factor)
    return _ingest_response(result)


@app.post("/api/v1/documents/upload", response_model=IngestDocResponse)
async def upload_document(
    file: UploadFile = File(...),
    doc_id: Optional[str] = Form(default=None, max_length=256),
    replication_factor: Optional[int] = Form(default=None, ge=1, le=16),
    metadata: Optional[str] = Form(default=None, description="JSON object merged into the document metadata"),
):
    """Ingests an uploaded .txt/.md/.pdf file. doc_id defaults to a slug of the filename."""
    max_bytes = int(config.MAX_UPLOAD_MB * 1024 * 1024)
    data = await file.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail=f"File exceeds MAX_UPLOAD_MB={config.MAX_UPLOAD_MB:g}.")
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    extra: Dict[str, Any] = {}
    if metadata:
        try:
            extra = json.loads(metadata)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="metadata must be a JSON object.")
        if not isinstance(extra, dict):
            raise HTTPException(status_code=400, detail="metadata must be a JSON object.")

    try:
        text = await asyncio.to_thread(extract_text, file.filename or "", data, file.content_type)
    except UnsupportedFileType as e:
        raise HTTPException(status_code=415, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not read '{file.filename}': {e}")
    if not text:
        raise HTTPException(status_code=422, detail="No extractable text in file (a scanned PDF needs OCR first).")

    meta = {**extra, "source": "upload", "filename": file.filename, "content_type": file.content_type, "size_bytes": len(data)}
    result = await coordinator.ingest(doc_id or doc_id_from_filename(file.filename or ""), text, meta, replication_factor)
    return _ingest_response(result, characters=len(text))


@app.get("/api/v1/documents")
async def list_documents(limit: int = Query(default=100, ge=1, le=1000), offset: int = Query(default=0, ge=0)):
    """Lists documents (newest first) with version, chunk count, metadata, replica placement and replication health."""
    catalog = await coordinator.list_documents()
    docs = catalog["documents"]
    return {"total": len(docs), "limit": limit, "offset": offset,
            "documents": docs[offset:offset + limit], "errors": catalog["errors"]}


@app.get("/api/v1/documents/{doc_id}")
async def get_document(doc_id: str):
    """One document's catalog entry plus its chunk texts."""
    doc = await coordinator.get_document(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found on any healthy shard.")
    return doc


@app.delete("/api/v1/documents/{doc_id}")
async def delete_document(doc_id: str):
    """Removes a document from every healthy shard."""
    return await coordinator.delete(doc_id)


@app.post("/api/v1/query", response_model=DistributedQueryResponse)
async def query_distributed_rag(req: DistributedQueryRequest):
    """Distributed scatter-gather query across shard nodes with caching, score merging, and LLM synthesis."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    start_time = time.perf_counter()

    cached_res = await asyncio.to_thread(coordinator.cache.get, req.query, req.top_k)
    if cached_res:
        RAG_CACHE_HITS.labels(status="hit").inc()
        cached_res["cached"] = True
        return DistributedQueryResponse(**cached_res)
    RAG_CACHE_HITS.labels(status="miss").inc()

    merged_results, telemetry = await _retrieve(req.query, req.top_k)

    # Blocking SDK call, so run it in a thread
    with span("llm_generate", provider=llm_generator.provider, chunks=len(merged_results)):
        llm_resp: RAGResponse = await asyncio.to_thread(llm_generator.generate_answer, req.query, merged_results)

    RAG_QUERY_LATENCY.observe(time.perf_counter() - start_time)

    response_dict = {
        "query": req.query,
        "answer": llm_resp.answer,
        "sources": _format_sources(llm_resp.sources),
        "telemetry": telemetry,
        "provider_used": llm_resp.provider_used,
        "cached": False
    }

    # Don't cache degraded answers: they may be missing results from a shard that is down
    if not telemetry.get("degraded"):
        await asyncio.to_thread(coordinator.cache.set, req.query, req.top_k, response_dict)

    return DistributedQueryResponse(**response_dict)


@app.post("/api/v1/query/stream")
async def query_stream(req: DistributedQueryRequest):
    """Same as /query, streamed as Server-Sent Events.

    Events: `retrieval` {sources, telemetry, cached} → `token` {text}… (`reset` {} = discard text so far)
    → `done` {provider_used, model_name, latency_ms, cached}. On failure: `error` {message}.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    async def stream():
        start_time = time.perf_counter()
        try:
            cached_res = await asyncio.to_thread(coordinator.cache.get, req.query, req.top_k)
            if cached_res:
                RAG_CACHE_HITS.labels(status="hit").inc()
                yield sse("retrieval", {"sources": cached_res["sources"], "telemetry": cached_res["telemetry"], "cached": True})
                yield sse("token", {"text": cached_res["answer"]})
                yield sse("done", {"provider_used": cached_res["provider_used"], "model_name": cached_res.get("model_name", ""),
                                   "latency_ms": round((time.perf_counter() - start_time) * 1000.0, 2), "cached": True})
                return
            RAG_CACHE_HITS.labels(status="miss").inc()

            merged_results, telemetry = await _retrieve(req.query, req.top_k)
            sources = _format_sources(merged_results)
            yield sse("retrieval", {"sources": sources, "telemetry": telemetry, "cached": False})

            # Bridge the blocking provider stream (worker thread) onto the event loop
            loop = asyncio.get_running_loop()
            queue: asyncio.Queue = asyncio.Queue()
            stop = threading.Event()

            def produce():
                try:
                    for event in llm_generator.stream_answer(req.query, merged_results, should_stop=stop.is_set):
                        loop.call_soon_threadsafe(queue.put_nowait, event)
                except Exception as e:
                    loop.call_soon_threadsafe(queue.put_nowait, e)
                finally:
                    loop.call_soon_threadsafe(queue.put_nowait, None)

            producer = asyncio.create_task(asyncio.to_thread(produce))
            parts: List[str] = []
            done: Optional[StreamEvent] = None
            try:
                with span("llm_generate_stream", provider=llm_generator.provider, chunks=len(merged_results)):
                    while (event := await queue.get()) is not None:
                        if isinstance(event, Exception):
                            raise event
                        if event.type == "token":
                            parts.append(event.text)
                            yield sse("token", {"text": event.text})
                        elif event.type == "reset":
                            parts.clear()
                            yield sse("reset", {})
                        elif event.type == "done":
                            done = event
            finally:
                stop.set()  # also runs when the client disconnects mid-stream
                await producer

            latency = time.perf_counter() - start_time
            RAG_QUERY_LATENCY.observe(latency)
            yield sse("done", {"provider_used": done.provider_used if done else "", "model_name": done.model_name if done else "",
                               "latency_ms": round(latency * 1000.0, 2), "cached": False})

            if done and not telemetry.get("degraded"):
                await asyncio.to_thread(coordinator.cache.set, req.query, req.top_k, {
                    "query": req.query, "answer": "".join(parts), "sources": sources, "telemetry": telemetry,
                    "provider_used": done.provider_used, "model_name": done.model_name, "cached": False,
                })
        except Exception as e:
            logger.error(f"Streaming query failed: {e}")
            yield sse("error", {"message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/v1/cluster/status")
async def get_cluster_status():
    """Returns cluster nodes, health, ring key distribution %, vector counts and the last repair pass."""
    return coordinator.status_snapshot()


@app.get("/api/v1/cluster/events")
async def cluster_events(request: Request):
    """Server-Sent Events stream of cluster activity.

    On connect: one `cluster_status` snapshot, then recent notable events (replayed, same ids). Live events after that:
    `cluster_status` (every heartbeat), `node_down`, `node_up`, `repair_completed`, `document_ingested`,
    `document_deleted`, `fault_injected`. A `: keepalive` comment is sent every 15s of silence.
    """
    queue = coordinator.events.subscribe()

    async def stream():
        try:
            yield sse("cluster_status", coordinator.status_snapshot())
            for event in list(coordinator.events.history):
                yield sse(event.type, {**event.data, "timestamp": event.timestamp}, event.id)
            while not await request.is_disconnected():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield sse(event.type, {**event.data, "timestamp": event.timestamp}, event.id)
        finally:
            coordinator.events.unsubscribe(queue)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/v1/cluster/repair")
async def trigger_repair():
    """Runs an anti-entropy repair pass now and returns its summary."""
    return await coordinator.repair()


@app.get("/api/v1/chaos")
async def chaos_status():
    """Whether fault injection is enabled, and the current fault on each shard."""
    return {
        "enabled": config.CHAOS_ENABLED,
        "shards": {s: coordinator.faults.get(s, {"mode": "none", "latency_ms": 0}) for s in coordinator.nodes},
    }


@app.post("/api/v1/chaos/shards/{shard_id}")
async def inject_fault(shard_id: str, req: FaultInjectionRequest):
    """Dev-only. mode: `down` (shard fails every RPC), `slow` (+latency_ms per RPC), `wipe` (clear its index, like a
    crash-restart), `none` (recover). Requires CHAOS_ENABLED=true on the coordinator and the shards."""
    _require_chaos()
    return await coordinator.set_fault(shard_id, req.mode, req.latency_ms)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

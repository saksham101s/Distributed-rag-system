import asyncio
import json
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import numpy as np

from proto_gen import vector_service_pb2, vector_service_pb2_grpc
from shared.faiss_store import SearchResult
from shared.config import config

logger = logging.getLogger("ScatterGather")


class ScatterGatherEngine:
    """Executes parallel scatter-gather queries across vector shard nodes with partial failure isolation."""

    def __init__(self, timeout_seconds: float = 1.5):
        self.timeout_seconds = timeout_seconds

    async def execute_scatter_gather(
        self,
        query_vector: np.ndarray,
        shard_stubs: Dict[str, vector_service_pb2_grpc.VectorStoreServiceStub],
        top_k: int = 5
    ) -> Tuple[List[SearchResult], Dict[str, Any]]:
        """Fans out SearchVector gRPC call to all active shard nodes concurrently, merges top-K, and handles timeouts gracefully."""
        start_time = time.perf_counter()
        
        if not shard_stubs:
            logger.warning("ScatterGather: No active shard stubs available!")
            return [], {"total_scatter_latency_ms": 0.0, "shard_metrics": {}, "degraded": True, "errors": ["No active shards"]}

        # Prepare gRPC search request
        req = vector_service_pb2.SearchRequest(
            shard_id="coordinator_fanout",
            query_vector=query_vector.tolist(),
            top_k=top_k
        )

        async def _query_single_shard(shard_id: str, stub: vector_service_pb2_grpc.VectorStoreServiceStub):
            shard_start = time.perf_counter()
            try:
                # Wrap with asyncio.wait_for to enforce strict shard timeout
                resp: vector_service_pb2.SearchResponse = await asyncio.wait_for(
                    stub.SearchVector(req),
                    timeout=self.timeout_seconds
                )
                duration_ms = (time.perf_counter() - shard_start) * 1000.0
                return shard_id, resp, duration_ms, None
            except asyncio.TimeoutError:
                duration_ms = (time.perf_counter() - shard_start) * 1000.0
                logger.warning(f"ScatterGather: Shard '{shard_id}' TIMED OUT after {duration_ms:.2f}ms")
                return shard_id, None, duration_ms, "Timeout"
            except Exception as e:
                duration_ms = (time.perf_counter() - shard_start) * 1000.0
                logger.error(f"ScatterGather: Shard '{shard_id}' RPC FAILED ({e}) after {duration_ms:.2f}ms")
                return shard_id, None, duration_ms, str(e)

        # Launch concurrent async scatter tasks across all active shards
        tasks = [_query_single_shard(shard_id, stub) for shard_id, stub in shard_stubs.items()]
        results_from_shards = await asyncio.gather(*tasks, return_exceptions=True)

        all_retrieved_results: List[SearchResult] = []
        shard_metrics: Dict[str, Any] = {}
        is_degraded = False
        errors_list = []

        for item in results_from_shards:
            if isinstance(item, Exception):
                is_degraded = True
                errors_list.append(str(item))
                continue
                
            shard_id, response, duration_ms, err = item
            
            if err:
                is_degraded = True
                errors_list.append(f"{shard_id}: {err}")
                shard_metrics[shard_id] = {
                    "status": "failed",
                    "error": err,
                    "latency_ms": round(duration_ms, 2),
                    "items_retrieved": 0
                }
            elif response:
                shard_items = []
                for pb_item in response.results:
                    meta = json.loads(pb_item.metadata_json) if pb_item.metadata_json else {}
                    sr = SearchResult(
                        chunk_id=pb_item.chunk_id,
                        doc_id=pb_item.doc_id,
                        text=pb_item.text,
                        score=pb_item.score,
                        chunk_index=pb_item.chunk_index,
                        metadata=meta,
                        shard_id=shard_id
                    )
                    shard_items.append(sr)
                    all_retrieved_results.append(sr)

                shard_metrics[shard_id] = {
                    "status": "success",
                    "latency_ms": round(response.latency_ms or duration_ms, 2),
                    "items_retrieved": len(shard_items)
                }

        # Global Merge & Re-rank by inner-product / cosine similarity score descending
        all_retrieved_results.sort(key=lambda x: x.score, reverse=True)
        top_k_merged = all_retrieved_results[:top_k]

        total_elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        telemetry = {
            "total_scatter_latency_ms": round(total_elapsed_ms, 2),
            "shards_queried": len(shard_stubs),
            "shards_successful": sum(1 for m in shard_metrics.values() if m.get("status") == "success"),
            "degraded": is_degraded,
            "errors": errors_list,
            "shard_metrics": shard_metrics
        }

        return top_k_merged, telemetry

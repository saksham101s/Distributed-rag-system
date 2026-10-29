"""Coordinator-level tests: ingest upsert, replica dedup on query, and anti-entropy repair after a shard restart."""
import asyncio

import grpc
import pytest

from proto_gen import vector_service_pb2_grpc
from shard_node.shard_service import VectorStoreServicer
from shared.embeddings import embeddings_engine
from coordinator.coordinator_service import CoordinatorState

SHARDS = ["shard_a", "shard_b", "shard_c"]
DOCS = {
    "doc_raft": "Raft elects a leader and replicates a log to followers.",
    "doc_faiss": "FAISS performs fast inner product search over dense vectors.",
    "doc_grpc": "gRPC runs over HTTP/2 and multiplexes many calls on one connection.",
    "doc_redis": "Redis caches query results with a time to live.",
}


async def _start_shard(shard_id: str, port: int = 0):
    server = grpc.aio.server()
    servicer = VectorStoreServicer(shard_id=shard_id)
    vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port(f"127.0.0.1:{port}")
    await server.start()
    return server, servicer, port


@pytest.fixture
async def cluster():
    servers, servicers, ports = {}, {}, {}
    for s in SHARDS:
        servers[s], servicers[s], ports[s] = await _start_shard(s)
    coord = CoordinatorState()
    for s in SHARDS:
        await coord.register_node(s, "127.0.0.1", ports[s])
    yield coord, servers, servicers, ports
    await coord.close()
    for server in servers.values():
        await server.stop(0)


def _holders(servicers, doc_id):
    return sorted(s for s, sv in servicers.items() if doc_id in sv.vector_store.docs)


async def test_ingest_is_upsert_and_reports_shards(cluster):
    coord, _, servicers, _ = cluster
    first = await coord.ingest("doc_raft", DOCS["doc_raft"], replication_factor=2)
    assert len(first.succeeded_shards) == 2 and not first.failed_shards
    assert _holders(servicers, "doc_raft") == sorted(first.target_shards)

    await coord.ingest("doc_raft", DOCS["doc_raft"] + " Terms increase monotonically.", replication_factor=2)
    total = sum(sv.vector_store.count() for sv in servicers.values())
    assert total == 2  # one chunk x two replicas, no duplicates from the first write


async def test_query_results_are_deduplicated(cluster):
    coord, _, _, _ = cluster
    for doc_id, text in DOCS.items():
        await coord.ingest(doc_id, text, replication_factor=2)
    stubs = await coord.get_active_stubs()
    results, telemetry = await coord.scatter_gather.execute_scatter_gather(
        embeddings_engine.embed_text("How does Raft replicate its log?"), stubs, top_k=4)
    assert len({r.chunk_id for r in results}) == len(results) == 4
    assert results[0].doc_id == "doc_raft"
    assert telemetry["candidates_retrieved"] > telemetry["unique_results"]


async def test_repair_restores_replicas_after_shard_restart(cluster):
    coord, servers, servicers, ports = cluster
    for doc_id, text in DOCS.items():
        await coord.ingest(doc_id, text, replication_factor=2)

    # Crash a shard that holds data, then let the heartbeat notice
    victim = next(s for s in SHARDS if servicers[s].vector_store.count())
    lost = [d for d in DOCS if d in servicers[victim].vector_store.docs]
    await servers[victim].stop(0)
    assert await coord.check_node_health() is True
    assert victim not in coord.ring.get_all_nodes()

    # Repair re-replicates to the surviving shards so every doc has 2 copies again
    await coord.repair()
    for doc_id in DOCS:
        assert len(_holders({s: sv for s, sv in servicers.items() if s != victim}, doc_id)) == 2

    # Restart the victim empty on the same port; repair moves its share back and trims the temporary copies
    servers[victim], servicers[victim], _ = await _start_shard(victim, ports[victim])
    for _ in range(40):
        await coord.check_node_health()
        if victim in coord.ring.get_all_nodes():
            break
        await asyncio.sleep(0.25)
    assert victim in coord.ring.get_all_nodes()
    summary = await coord.repair()
    assert not summary["errors"]
    for doc_id in DOCS:
        assert _holders(servicers, doc_id) == sorted(coord.ring.get_nodes(doc_id, 2))
    assert all(d in servicers[victim].vector_store.docs for d in lost)


async def test_repair_does_not_resurrect_deleted_doc(cluster):
    coord, servers, servicers, ports = cluster
    result = await coord.ingest("doc_grpc", DOCS["doc_grpc"], replication_factor=2)
    down = result.target_shards[0]

    # One replica is unreachable while the delete happens, so it keeps its copy
    await servers[down].stop(0)
    await coord.check_node_health()
    await coord.delete("doc_grpc")
    stale_store = servicers[down].vector_store

    servers[down], servicers[down], _ = await _start_shard(down, ports[down])
    servicers[down].vector_store = stale_store  # simulate a shard that came back with its old data
    for _ in range(40):
        await coord.check_node_health()
        if down in coord.ring.get_all_nodes():
            break
        await asyncio.sleep(0.25)
    await coord.repair()
    assert _holders(servicers, "doc_grpc") == []

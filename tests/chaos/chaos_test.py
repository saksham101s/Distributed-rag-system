import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import time
import json
import asyncio
import logging
import subprocess

from proto_gen import vector_service_pb2, vector_service_pb2_grpc
from shared.embeddings import embeddings_engine
from coordinator.coordinator_service import CoordinatorState

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ChaosTestRunner")


async def run_chaos_test():
    logger.info("=== STARTING CHAOS FAULT-TOLERANCE SUITE ===")

    # 0. Warmup embedding model in main process first
    logger.info("Warming up local embedding model in main process...")
    _ = embeddings_engine.embed_text("Warmup prompt")
    logger.info("Embedding model warmed up.")
    
    # 1. Start 3 background gRPC shard node subprocesses
    ports = {"shard_0": 50151, "shard_1": 50152, "shard_2": 50153}
    processes = {}
    
    python_exe = sys.executable
    shard_script = os.path.join(os.path.dirname(__file__), "..", "..", "shard_node", "shard_service.py")

    # Prepare environment with PYTHONPATH pointing to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")

    for shard_id, port in ports.items():
        cmd = [python_exe, shard_script, "--shard-id", shard_id, "--port", str(port)]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        processes[shard_id] = proc
        logger.info(f"Spawned {shard_id} process (PID: {proc.pid}) on port {port}")

    try:
        # Wait for gRPC servers to initialize and become ready
        logger.info("Waiting for gRPC shard nodes to finish initialization...")
        coord = CoordinatorState()
        for shard_id, port in ports.items():
            await coord.register_node(shard_id, "127.0.0.1", port)

        # Poll HealthCheck until all 3 nodes respond
        nodes_ready = False
        for attempt in range(35):
            stubs = await coord.get_active_stubs()
            healthy_count = 0
            for s_id, stub in stubs.items():
                try:
                    resp = await stub.HealthCheck(vector_service_pb2.HealthCheckRequest(client_id="init_check"), timeout=1.0)
                    if resp.healthy:
                        healthy_count += 1
                except Exception:
                    pass
            if healthy_count == 3:
                logger.info("All 3 gRPC Shard Nodes are READY!")
                nodes_ready = True
                break
            await asyncio.sleep(1.0)

        if not nodes_ready:
            logger.error("Failed to start all 3 shard nodes within timeout.")
            for s_id, p in processes.items():
                stdout, stderr = p.communicate(timeout=1)
                logger.error(f"[{s_id}] stdout: {stdout.decode()}")
                logger.error(f"[{s_id}] stderr: {stderr.decode()}")
            raise RuntimeError("Shard nodes startup timed out.")

        ring_dist = coord.ring.ring_distribution()
        logger.info(f"Initial Ring Distribution: {ring_dist}")

        # 3. Ingest documents with replication factor N=2 through the coordinator's real write path
        test_docs = [
            ("doc_arch", "Distributed systems replicate vector indices to achieve fault tolerance."),
            ("doc_faiss", "FAISS provides fast inner product search on dense embedding matrices."),
            ("doc_grpc", "gRPC uses HTTP/2 multiplexing for efficient low-latency microservice RPCs."),
            ("doc_hash", "Consistent hashing maps keys to virtual nodes with minimal churn.")
        ]
        for doc_id, text in test_docs:
            result = await coord.ingest(doc_id, text, replication_factor=2)
            logger.info(f"Ingested '{doc_id}' -> {result.succeeded_shards} (failed: {result.failed_shards or 'none'})")
            assert len(result.succeeded_shards) == 2

        # 4. Verify baseline query on healthy cluster
        query_vec = embeddings_engine.embed_text("How do distributed systems achieve fault tolerance?")
        stubs = await coord.get_active_stubs()
        results, telemetry = await coord.scatter_gather.execute_scatter_gather(query_vec, stubs, top_k=3)
        logger.info(f"Baseline Query Successful! Results: {len(results)}, Shards hit: {telemetry['shards_successful']}/3")
        assert len(results) >= 1
        assert telemetry["degraded"] is False

        # 5. --- CHAOS INJECTION: KILL SHARD_1 WHILE QUERIES ARE IN FLIGHT ---
        # The stub map still contains shard_1 (the heartbeat hasn't noticed yet), exactly as in production
        all_stubs = await coord.get_active_stubs()

        async def query_burst(n: int):
            return await asyncio.gather(*(
                coord.scatter_gather.execute_scatter_gather(query_vec, all_stubs, top_k=3) for _ in range(n)
            ))

        burst = asyncio.create_task(query_burst(50))
        await asyncio.sleep(0)  # let the burst start sending
        logger.info("🔥 [CHAOS] SIGKILL shard_1 (PID: %d) mid-burst...", processes["shard_1"].pid)
        processes["shard_1"].kill()
        processes["shard_1"].wait()
        burst_results = await burst
        burst_results += await query_burst(5)  # queries issued after the crash, before the heartbeat reacts

        degraded = sum(1 for _, t in burst_results if t["degraded"])
        logger.info(f"Burst: {len(burst_results)} queries, {degraded} degraded, all answered: "
                    f"{all(len(r) >= 1 for r, _ in burst_results)}")
        assert all(len(r) >= 1 for r, _ in burst_results), "a query returned nothing during the crash"
        assert degraded >= 1, "expected at least one query to observe the dead shard"
        logger.info("✅ SUCCESS: every query was answered from replicas while shard_1 died.")

        # Heartbeat removes shard_1 from the ring; repair restores 2 copies of every doc on the survivors
        await coord.check_node_health()
        assert "shard_1" not in coord.ring.get_all_nodes()
        summary = await coord.repair()
        logger.info(f"Repair after crash: {summary}")
        assert not summary["errors"]

        # 6. --- SELF-HEALING: RESTART SHARD_1 (it comes back empty) ---
        logger.info("🌱 [SELF-HEALING] Restarting shard_1...")
        cmd_1 = [python_exe, shard_script, "--shard-id", "shard_1", "--port", str(ports["shard_1"])]
        processes["shard_1"] = subprocess.Popen(cmd_1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        for _ in range(30):
            await coord.check_node_health()
            if "shard_1" in coord.ring.get_all_nodes():
                break
            await asyncio.sleep(1.0)
        assert "shard_1" in coord.ring.get_all_nodes()
        logger.info("✅ SUCCESS: heartbeat restored shard_1 to the ring.")

        # 7. Repair moves shard_1's share back, so placement matches the ring again
        summary = await coord.repair()
        logger.info(f"Repair after recovery: {summary}")
        assert not summary["errors"]
        stubs = await coord.get_active_stubs()
        placement = {}
        for shard_id, stub in stubs.items():
            listing = await stub.ListDocuments(vector_service_pb2.ListDocumentsRequest())
            for d in listing.documents:
                placement.setdefault(d.doc_id, set()).add(shard_id)
        for doc_id, _ in test_docs:
            expected = set(coord.ring.get_nodes(doc_id, replication_factor=2))
            assert placement.get(doc_id) == expected, f"{doc_id}: on {placement.get(doc_id)}, ring says {expected}"
        logger.info("✅ SUCCESS: every document is back on exactly its ring-assigned replicas.")

    finally:
        # Cleanup processes
        for shard_id, proc in processes.items():
            if proc.poll() is None:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    asyncio.run(run_chaos_test())

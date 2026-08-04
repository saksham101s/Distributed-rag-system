import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import time
import json
import asyncio
import logging
import subprocess

from proto_gen import vector_service_pb2, vector_service_pb2_grpc
from shared.consistent_hash import ConsistentHashRing
from shared.document_processor import DocumentProcessor
from shared.embeddings import embeddings_engine
from coordinator.scatter_gather import ScatterGatherEngine
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

        # 3. Ingest documents with replication factor N=2
        doc_processor = DocumentProcessor()
        test_docs = [
            ("doc_arch", "Distributed systems replicate vector indices to achieve fault tolerance."),
            ("doc_faiss", "FAISS provides fast inner product search on dense embedding matrices."),
            ("doc_grpc", "gRPC uses HTTP/2 multiplexing for efficient low-latency microservice RPCs."),
            ("doc_hash", "Consistent hashing maps keys to virtual nodes with minimal churn.")
        ]

        for doc_id, text in test_docs:
            target_shards = coord.ring.get_nodes(doc_id, replication_factor=2)
            logger.info(f"Ingesting '{doc_id}' -> Target Shards (Primary + Replica): {target_shards}")
            
            chunks = doc_processor.process_document(doc_id, text)
            vecs = embeddings_engine.embed_batch([c.text for c in chunks])
            
            pb_chunks = [
                vector_service_pb2.VectorChunk(
                    chunk_id=chunks[0].chunk_id,
                    doc_id=doc_id,
                    text=text,
                    chunk_index=0,
                    embedding=vecs[0].tolist()
                )
            ]
            
            stubs = await coord.get_active_stubs()
            ingest_tasks = [
                stubs[s_id].IngestVector(vector_service_pb2.IngestVectorRequest(shard_id=s_id, chunks=pb_chunks))
                for s_id in target_shards if s_id in stubs
            ]
            await asyncio.gather(*ingest_tasks)

        # 4. Verify baseline query on healthy cluster
        query_vec = embeddings_engine.embed_text("How do distributed systems achieve fault tolerance?")
        stubs = await coord.get_active_stubs()
        results, telemetry = await coord.scatter_gather.execute_scatter_gather(query_vec, stubs, top_k=3)
        
        logger.info(f"Baseline Query Successful! Results count: {len(results)}, Shards hit: {telemetry['shards_successful']}/3")
        assert len(results) >= 1
        assert telemetry["degraded"] is False

        # 5. --- CHAOS INJECTION: KILL SHARD_1 ABRUPTLY ---
        logger.info("🔥 [CHAOS] KILLING shard_1 (PID: %d)...", processes["shard_1"].pid)
        processes["shard_1"].kill()
        processes["shard_1"].wait()
        logger.info("🔥 [CHAOS] shard_1 TERMINATED!")

        # Trigger heartbeat check to update hash ring
        await coord.check_node_health()
        logger.info(f"Post-kill Hash Ring Active Nodes: {coord.ring.get_all_nodes()}")
        assert "shard_1" not in coord.ring.get_all_nodes()

        # 6. Re-query system mid-failure
        active_stubs = await coord.get_active_stubs()
        logger.info(f"Executing query during shard_1 outage across active stubs: {list(active_stubs.keys())}")
        
        results_post_kill, telemetry_post_kill = await coord.scatter_gather.execute_scatter_gather(query_vec, active_stubs, top_k=3)
        logger.info(f"Post-kill Query Results: {len(results_post_kill)} retrieved. Degraded flag: {telemetry_post_kill['degraded']}")
        
        # Verify system still answered!
        assert len(results_post_kill) >= 1
        assert results_post_kill[0].doc_id in ["doc_arch", "doc_faiss", "doc_grpc", "doc_hash"]
        logger.info("✅ SUCCESS: Distributed system served query despite mid-request node crash!")

        # 7. --- SELF-HEALING: RESTART SHARD_1 ---
        logger.info("🌱 [SELF-HEALING] Restarting shard_1...")
        cmd_1 = [python_exe, shard_script, "--shard-id", "shard_1", "--port", str(ports["shard_1"])]
        proc_1_new = subprocess.Popen(cmd_1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        processes["shard_1"] = proc_1_new
        
        # Poll HealthCheck until restored
        for _ in range(15):
            await coord.check_node_health()
            if "shard_1" in coord.ring.get_all_nodes():
                break
            await asyncio.sleep(1.0)

        logger.info(f"Post-recovery Ring Active Nodes: {coord.ring.get_all_nodes()}")
        assert "shard_1" in coord.ring.get_all_nodes()
        logger.info("✅ SUCCESS: Self-healing heartbeat restored shard_1 to cluster!")

    finally:
        # Cleanup processes
        for shard_id, proc in processes.items():
            if proc.poll() is None:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    asyncio.run(run_chaos_test())

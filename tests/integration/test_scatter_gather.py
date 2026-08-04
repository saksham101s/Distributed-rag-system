import pytest
import asyncio
import numpy as np
import grpc

from proto_gen import vector_service_pb2, vector_service_pb2_grpc
from shard_node.shard_service import VectorStoreServicer
from shared.document_processor import DocumentProcessor
from shared.embeddings import embeddings_engine
from coordinator.scatter_gather import ScatterGatherEngine


@pytest.mark.asyncio
async def test_grpc_shard_node_ingest_and_search():
    # 1. Start local gRPC server on port 50091
    server = grpc.aio.server()
    servicer = VectorStoreServicer(shard_id="test_shard_91")
    vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    try:
        # 2. Connect via gRPC stub
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        stub = vector_service_pb2_grpc.VectorStoreServiceStub(channel)

        # 3. Test HealthCheck
        health_resp = await stub.HealthCheck(vector_service_pb2.HealthCheckRequest(client_id="test"))
        assert health_resp.healthy is True
        assert health_resp.shard_id == "test_shard_91"

        # 4. Prepare vector ingest
        processor = DocumentProcessor()
        chunks = processor.process_document("doc_g1", "gRPC inter-service communication is fast and binary encoded.")
        vecs = embeddings_engine.embed_batch([c.text for c in chunks])

        pb_chunks = [
            vector_service_pb2.VectorChunk(
                chunk_id=chunks[0].chunk_id,
                doc_id=chunks[0].doc_id,
                text=chunks[0].text,
                chunk_index=0,
                embedding=vecs[0].tolist()
            )
        ]
        
        ingest_resp = await stub.IngestVector(vector_service_pb2.IngestVectorRequest(shard_id="test_shard_91", chunks=pb_chunks))
        assert ingest_resp.success is True
        assert ingest_resp.inserted_count == 1

        # 5. Test vector search
        query_vec = vecs[0]
        search_resp = await stub.SearchVector(vector_service_pb2.SearchRequest(query_vector=query_vec.tolist(), top_k=2))
        assert len(search_resp.results) == 1
        assert search_resp.results[0].doc_id == "doc_g1"
        assert search_resp.results[0].score > 0.8
        await channel.close()
    finally:
        await server.stop(0)


@pytest.mark.asyncio
async def test_scatter_gather_fanout_and_partial_failure():
    # 1. Start 2 healthy gRPC shard nodes on dynamic ports
    server1 = grpc.aio.server()
    servicer1 = VectorStoreServicer(shard_id="shard_alpha")
    vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicer1, server1)
    port1 = server1.add_insecure_port("127.0.0.1:0")
    await server1.start()

    server2 = grpc.aio.server()
    servicer2 = VectorStoreServicer(shard_id="shard_beta")
    vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicer2, server2)
    port2 = server2.add_insecure_port("127.0.0.1:0")
    await server2.start()

    try:
        channel1 = grpc.aio.insecure_channel(f"127.0.0.1:{port1}")
        stub1 = vector_service_pb2_grpc.VectorStoreServiceStub(channel1)

        channel2 = grpc.aio.insecure_channel(f"127.0.0.1:{port2}")
        stub2 = vector_service_pb2_grpc.VectorStoreServiceStub(channel2)

        # Ingest doc into shard_alpha
        processor = DocumentProcessor()
        chunks1 = processor.process_document("doc_alpha", "Alpha shard holds document about distributed systems.")
        vecs1 = embeddings_engine.embed_batch([c.text for c in chunks1])
        await stub1.IngestVector(vector_service_pb2.IngestVectorRequest(
            shard_id="shard_alpha",
            chunks=[vector_service_pb2.VectorChunk(chunk_id=chunks1[0].chunk_id, doc_id="doc_alpha", text=chunks1[0].text, chunk_index=0, embedding=vecs1[0].tolist())]
        ))

        # Ingest doc into shard_beta
        chunks2 = processor.process_document("doc_beta", "Beta shard holds document about neural networks.")
        vecs2 = embeddings_engine.embed_batch([c.text for c in chunks2])
        await stub2.IngestVector(vector_service_pb2.IngestVectorRequest(
            shard_id="shard_beta",
            chunks=[vector_service_pb2.VectorChunk(chunk_id=chunks2[0].chunk_id, doc_id="doc_beta", text=chunks2[0].text, chunk_index=0, embedding=vecs2[0].tolist())]
        ))

        # Channel to non-existent dead port (simulating a dead shard node)
        dead_channel = grpc.aio.insecure_channel("127.0.0.1:59999")
        dead_stub = vector_service_pb2_grpc.VectorStoreServiceStub(dead_channel)

        stubs_map = {
            "shard_alpha": stub1,
            "shard_beta": stub2,
            "shard_dead": dead_stub
        }

        engine = ScatterGatherEngine(timeout_seconds=0.5)
        query_vec = vecs1[0]
        results, telemetry = await engine.execute_scatter_gather(query_vec, stubs_map, top_k=5)

        # Verify scatter gather gathered results despite shard_dead failing!
        assert len(results) >= 1
        assert results[0].shard_id in ["shard_alpha", "shard_beta"]
        assert telemetry["degraded"] is True
        assert telemetry["shards_successful"] == 2
        assert "shard_dead" in telemetry["shard_metrics"]
        assert telemetry["shard_metrics"]["shard_dead"]["status"] == "failed"

        await channel1.close()
        await channel2.close()
        await dead_channel.close()
    finally:
        await server1.stop(0)
        await server2.stop(0)

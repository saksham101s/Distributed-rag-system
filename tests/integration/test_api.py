"""HTTP-level tests for the Phase A endpoints, against in-process shards (no lifespan, no Redis, mock LLM)."""
import asyncio
import json

import grpc
import httpx
import pytest

from proto_gen import vector_service_pb2_grpc
from shard_node.shard_service import VectorStoreServicer
from shared.config import config
import coordinator.coordinator_service as svc

SHARDS = ["shard_a", "shard_b", "shard_c"]


def _tiny_pdf(text: str) -> bytes:
    """Builds a minimal one-page PDF whose content stream draws `text`."""
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    out += b"".join(b"%010d 00000 n \n" % o for o in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    return bytes(out)


def _parse_sse(body: str):
    events = []
    for frame in body.strip().split("\n\n"):
        name, data = None, None
        for line in frame.splitlines():
            if line.startswith("event: "):
                name = line[7:]
            elif line.startswith("data: "):
                data = json.loads(line[6:])
        if name:
            events.append((name, data))
    return events


@pytest.fixture
async def api(monkeypatch):
    servers, servicers = {}, {}
    coord = svc.CoordinatorState()
    for s in SHARDS:
        server = grpc.aio.server()
        servicers[s] = VectorStoreServicer(shard_id=s, chaos_enabled=True)
        vector_service_pb2_grpc.add_VectorStoreServiceServicer_to_server(servicers[s], server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers[s] = server
        await coord.register_node(s, "127.0.0.1", port)
    monkeypatch.setattr(svc, "coordinator", coord)
    monkeypatch.setattr(config, "CHAOS_ENABLED", True)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=svc.app), base_url="http://test") as client:
        yield client, coord, servicers
    await coord.close()
    for server in servers.values():
        await server.stop(0)


async def _ingest(client, doc_id, text):
    r = await client.post("/api/v1/documents/ingest", json={"doc_id": doc_id, "text": text})
    assert r.status_code == 200, r.text
    return r.json()


async def test_document_catalog_and_detail(api):
    client, _, _ = api
    await _ingest(client, "doc_raft", "Raft elects a leader and replicates a log to followers.")
    await _ingest(client, "doc_faiss", "FAISS performs fast inner product search over dense vectors.")

    r = await client.get("/api/v1/documents")
    body = r.json()
    assert body["total"] == 2
    newest = body["documents"][0]
    assert newest["doc_id"] == "doc_faiss"  # newest first
    assert newest["replication_status"] == "ok"
    assert sorted(x["shard_id"] for x in newest["replicas"]) == sorted(newest["target_shards"])

    r = await client.get("/api/v1/documents/doc_raft")
    detail = r.json()
    assert detail["chunks"][0]["text"].startswith("Raft elects")
    assert "embedding" not in detail["chunks"][0]

    assert (await client.get("/api/v1/documents/nope")).status_code == 404


async def test_upload_text_and_pdf(api):
    client, _, _ = api
    r = await client.post("/api/v1/documents/upload",
                          files={"file": ("Gossip Notes.md", b"# Gossip\nNodes exchange state with random peers.", "text/markdown")},
                          data={"metadata": json.dumps({"team": "infra"})})
    assert r.status_code == 200, r.text
    assert r.json()["doc_id"] == "gossip-notes"

    r = await client.post("/api/v1/documents/upload",
                          files={"file": ("vector clocks.pdf", _tiny_pdf("Vector clocks order events causally."), "application/pdf")})
    assert r.status_code == 200, r.text
    assert r.json()["characters"] > 10

    detail = (await client.get("/api/v1/documents/gossip-notes")).json()
    assert detail["metadata"]["team"] == "infra" and detail["metadata"]["filename"] == "Gossip Notes.md"
    pdf = (await client.get("/api/v1/documents/vector-clocks")).json()
    assert "Vector clocks" in pdf["chunks"][0]["text"]

    bad = await client.post("/api/v1/documents/upload", files={"file": ("image.png", b"\x89PNG....", "image/png")})
    assert bad.status_code == 415


async def test_query_stream_events(api):
    client, _, _ = api
    await _ingest(client, "doc_raft", "Raft elects a leader and replicates a log to followers.")

    r = await client.post("/api/v1/query/stream", json={"query": "How does Raft replicate?", "top_k": 3})
    assert r.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(r.text)
    names = [e[0] for e in events]
    assert names[0] == "retrieval" and names[-1] == "done"
    assert names.count("token") > 3
    assert events[0][1]["sources"][0]["doc_id"] == "doc_raft"
    answer = "".join(d["text"] for n, d in events if n == "token")
    assert "Mock LLM Response" in answer

    # Second identical query is served from the cache, still as a stream
    cached = _parse_sse((await client.post("/api/v1/query/stream", json={"query": "How does Raft replicate?", "top_k": 3})).text)
    assert cached[0][1]["cached"] is True and cached[-1][1]["cached"] is True
    assert "".join(d["text"] for n, d in cached if n == "token") == answer


async def test_chaos_down_wipe_and_recover(api):
    client, coord, servicers = api
    for i in range(4):
        await _ingest(client, f"doc_{i}", f"Document number {i} about distributed consensus and replication.")

    r = await client.post("/api/v1/chaos/shards/shard_a", json={"mode": "down"})
    assert r.status_code == 200 and r.json()["mode"] == "down"
    status = (await client.get("/api/v1/cluster/status")).json()
    assert next(n for n in status["nodes"] if n["shard_id"] == "shard_a")["healthy"] is False

    # Queries still answer while a shard is down
    q = (await client.post("/api/v1/query", json={"query": "consensus", "top_k": 4})).json()
    assert len(q["sources"]) == 4

    assert (await client.post("/api/v1/chaos/shards/shard_a", json={"mode": "none"})).status_code == 200
    for _ in range(20):
        if "shard_a" in coord.ring.get_all_nodes():
            break
        await asyncio.sleep(0.25)
        await coord.check_node_health()
    assert "shard_a" in coord.ring.get_all_nodes()

    # Wipe = crash-restart with empty memory; repair refills it
    victim = next(s for s in SHARDS if servicers[s].vector_store.count())
    r = await client.post(f"/api/v1/chaos/shards/{victim}", json={"mode": "wipe"})
    assert r.json()["vector_count"] == 0
    under = [d for d in (await client.get("/api/v1/documents")).json()["documents"] if d["replication_status"] != "ok"]
    assert under, "wiping a shard should leave some docs under-replicated"
    await client.post("/api/v1/cluster/repair")
    docs = (await client.get("/api/v1/documents")).json()["documents"]
    assert all(d["replication_status"] == "ok" for d in docs)

    types = [e.type for e in coord.events.history]
    assert {"node_down", "node_up", "fault_injected", "repair_completed", "document_ingested"} <= set(types)


async def test_chaos_disabled_returns_403(api, monkeypatch):
    client, _, _ = api
    monkeypatch.setattr(config, "CHAOS_ENABLED", False)
    r = await client.post("/api/v1/chaos/shards/shard_a", json={"mode": "down"})
    assert r.status_code == 403


async def test_api_key_auth(api, monkeypatch):
    client, _, _ = api
    monkeypatch.setattr(config, "API_KEY", "s3cret")

    assert (await client.get("/api/v1/cluster/status")).status_code == 401
    assert (await client.get("/api/v1/cluster/status", headers={"Authorization": "Bearer wrong"})).status_code == 401
    assert (await client.get("/api/v1/cluster/status", headers={"Authorization": "Bearer s3cret"})).status_code == 200
    assert (await client.get("/api/v1/cluster/status", headers={"X-API-Key": "s3cret"})).status_code == 200
    # The query-string key is only for the event stream (EventSource can't set headers)
    assert (await client.get("/api/v1/cluster/status?api_key=s3cret")).status_code == 401
    assert (await client.get("/api/v1/cluster/events")).status_code == 401
    # Preflight still works without a key
    pre = await client.options("/api/v1/query", headers={"Origin": "http://x", "Access-Control-Request-Method": "POST"})
    assert pre.status_code == 200

    monkeypatch.setattr(config, "API_KEY", "")
    assert (await client.get("/api/v1/cluster/status")).status_code == 200

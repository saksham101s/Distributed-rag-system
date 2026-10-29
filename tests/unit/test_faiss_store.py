import numpy as np
import pytest

from shared.document_processor import DocumentChunk
from shared.faiss_store import FAISSVectorStore, SearchResult
from coordinator.scatter_gather import ScatterGatherEngine

DIM = 8


def _vec(seed: int) -> np.ndarray:
    v = np.random.default_rng(seed).standard_normal(DIM).astype(np.float32)
    return v / np.linalg.norm(v)


def _chunks(doc_id: str, n: int):
    return [DocumentChunk(chunk_id=f"{doc_id}-{i}", doc_id=doc_id, text=f"{doc_id} part {i}", chunk_index=i) for i in range(n)]


def test_upsert_replaces_previous_version():
    store = FAISSVectorStore(dim=DIM)
    store.upsert_document("d1", _chunks("d1", 3), np.stack([_vec(i) for i in range(3)]), version=1)
    assert store.count() == 3

    store.upsert_document("d1", _chunks("d1", 2), np.stack([_vec(10 + i) for i in range(2)]), version=2)
    assert store.count() == 2
    assert store.list_documents()[0].version == 2


def test_stale_write_is_ignored():
    store = FAISSVectorStore(dim=DIM)
    store.upsert_document("d1", _chunks("d1", 1), np.stack([_vec(1)]), version=5)
    applied = store.upsert_document("d1", _chunks("d1", 4), np.stack([_vec(i) for i in range(4)]), version=3)
    assert applied is False
    assert store.count() == 1


def test_delete_below_version():
    store = FAISSVectorStore(dim=DIM)
    store.upsert_document("d1", _chunks("d1", 2), np.stack([_vec(1), _vec(2)]), version=5)
    assert store.delete_by_doc_id("d1", below_version=5) == 0
    assert store.delete_by_doc_id("d1", below_version=6) == 2
    assert store.count() == 0
    assert store.list_documents() == []


def test_get_document_round_trips_embeddings():
    store = FAISSVectorStore(dim=DIM)
    vecs = np.stack([_vec(1), _vec(2)])
    store.upsert_document("d1", _chunks("d1", 2), vecs, version=7, replication_factor=2)
    info, chunks, out = store.get_document("d1")
    assert info.version == 7 and info.replication_factor == 2
    assert [c.chunk_index for c in chunks] == [0, 1]
    np.testing.assert_allclose(out, vecs, rtol=1e-6)


def test_dimension_mismatch_raises():
    store = FAISSVectorStore(dim=DIM)
    with pytest.raises(ValueError):
        store.add_chunks(_chunks("d1", 1), np.zeros((1, DIM + 1), dtype=np.float32))
    with pytest.raises(ValueError):
        store.search(np.zeros(DIM - 1, dtype=np.float32))


def _hit(chunk_id, doc_id, score, shard, version=1):
    return SearchResult(chunk_id=chunk_id, doc_id=doc_id, text="t", score=score, chunk_index=0,
                        metadata={}, shard_id=shard, doc_version=version)


def test_merge_dedups_replicas_and_drops_stale_versions():
    hits = [
        _hit("a", "doc1", 0.9, "shard_0"),
        _hit("a", "doc1", 0.9, "shard_1"),        # replica of the same chunk
        _hit("b", "doc2", 0.8, "shard_0", version=1),  # stale copy
        _hit("c", "doc2", 0.7, "shard_2", version=2),
        _hit("d", "doc3", 0.1, "shard_2"),
    ]
    merged = ScatterGatherEngine.merge_results(hits, top_k=5, score_threshold=0.2)
    assert [r.chunk_id for r in merged] == ["a", "c"]

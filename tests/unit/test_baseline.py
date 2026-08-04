import pytest
import numpy as np
from shared.document_processor import DocumentProcessor
from shared.embeddings import embeddings_engine
from shared.faiss_store import FAISSVectorStore
from shared.llm import LLMGenerator


def test_document_processor_chunking():
    processor = DocumentProcessor(chunk_size=100, chunk_overlap=20)
    sample_text = (
        "Distributed RAG systems scale vector search across multiple shard nodes. "
        "Each shard maintains a local FAISS index for high throughput ANN search. "
        "A coordinator node fans out queries using consistent hashing and gRPC RPCs. "
        "Replication ensures fault tolerance even when nodes crash mid-query."
    )
    chunks = processor.process_document(doc_id="doc_test_1", text=sample_text)
    
    assert len(chunks) > 0
    assert chunks[0].doc_id == "doc_test_1"
    assert chunks[0].chunk_index == 0
    assert "Distributed RAG" in chunks[0].text


def test_embeddings_and_faiss_retrieval():
    processor = DocumentProcessor(chunk_size=200, chunk_overlap=30)
    text1 = "Python and FastAPI allow high-performance async Web APIs."
    text2 = "Consistent hashing distributes keys evenly across ring nodes with minimal remapping."
    text3 = "FAISS is a C++ library for efficient similarity search of dense vectors."

    doc1_chunks = processor.process_document("doc_py", text1)
    doc2_chunks = processor.process_document("doc_hash", text2)
    doc3_chunks = processor.process_document("doc_faiss", text3)

    all_chunks = doc1_chunks + doc2_chunks + doc3_chunks
    texts = [c.text for c in all_chunks]
    
    embeddings = embeddings_engine.embed_batch(texts)
    assert embeddings.shape == (3, 384)
    # Check L2 normalization (norm should be ~1.0)
    norms = np.linalg.norm(embeddings, axis=1)
    np.testing.assert_allclose(norms, 1.0, rtol=1e-5)

    store = FAISSVectorStore(dim=384, shard_id="test_shard")
    store.add_chunks(all_chunks, embeddings)
    assert store.count() == 3

    # Query specifically about consistent hashing
    query_vec = embeddings_engine.embed_text("How does consistent hash routing work?")
    results = store.search(query_vec, top_k=2)

    assert len(results) == 2
    assert results[0].doc_id == "doc_hash"
    assert "Consistent hashing" in results[0].text
    assert results[0].score > 0.3


def test_llm_generator_mock():
    processor = DocumentProcessor(chunk_size=100, chunk_overlap=20)
    chunks = processor.process_document("doc_test", "Vector search is optimized using IndexFlatIP in FAISS.")
    embeddings = embeddings_engine.embed_batch([c.text for c in chunks])
    
    store = FAISSVectorStore(dim=384, shard_id="test_shard")
    store.add_chunks(chunks, embeddings)

    query_vec = embeddings_engine.embed_text("What index does FAISS use?")
    results = store.search(query_vec, top_k=1)

    llm = LLMGenerator(provider="mock")
    rag_resp = llm.generate_answer("What index does FAISS use?", results)

    assert rag_resp.query == "What index does FAISS use?"
    assert len(rag_resp.sources) == 1
    assert "Mock LLM Response" in rag_resp.answer
    assert rag_resp.sources[0].doc_id == "doc_test"

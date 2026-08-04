import logging
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.document_processor import DocumentProcessor, DocumentChunk
from shared.embeddings import embeddings_engine
from shared.faiss_store import FAISSVectorStore, SearchResult
from shared.llm import llm_generator, RAGResponse
from shared.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("BaselineRAG")

app = FastAPI(
    title="Baseline Single-Node RAG API",
    description="Single-node RAG pipeline demonstrating ingestion, embedding, FAISS index search, and LLM synthesis.",
    version="1.0.0"
)

# Global in-memory components for baseline node
doc_processor = DocumentProcessor(chunk_size=config.DEFAULT_CHUNK_SIZE, chunk_overlap=config.DEFAULT_CHUNK_OVERLAP)
vector_store = FAISSVectorStore(dim=config.EMBEDDING_DIM, shard_id="single_node_baseline")


class IngestRequest(BaseModel):
    doc_id: str
    text: str
    metadata: Optional[Dict[str, Any]] = None


class IngestResponse(BaseModel):
    doc_id: str
    chunks_created: int
    total_vectors_in_store: int


class QueryRequest(BaseModel):
    query: str
    top_k: int = 5


@app.post("/api/v1/documents/ingest", response_model=IngestResponse)
async def ingest_document(req: IngestRequest):
    """Ingests a text document: splits into chunks, computes embeddings, indexes in FAISS."""
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Document text cannot be empty.")
    
    chunks: List[DocumentChunk] = doc_processor.process_document(
        doc_id=req.doc_id, 
        text=req.text, 
        metadata=req.metadata
    )
    if not chunks:
        raise HTTPException(status_code=400, detail="No valid text chunks produced.")

    texts = [c.text for c in chunks]
    embeddings = embeddings_engine.embed_batch(texts)
    
    # Store in FAISS
    vector_store.add_chunks(chunks, embeddings)
    
    return IngestResponse(
        doc_id=req.doc_id,
        chunks_created=len(chunks),
        total_vectors_in_store=vector_store.count()
    )


@app.post("/api/v1/query", response_model=RAGResponse)
async def query_rag(req: QueryRequest):
    """Processes RAG user query: embeds query, searches FAISS, generates synthesized answer."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    # 1. Embed query
    query_vector = embeddings_engine.embed_text(req.query)
    
    # 2. Search FAISS index
    results: List[SearchResult] = vector_store.search(query_vector, top_k=req.top_k)
    
    # 3. LLM synthesis
    response: RAGResponse = llm_generator.generate_answer(req.query, results)
    return response


@app.get("/api/v1/health")
async def health_check():
    """Returns baseline server health status and index count."""
    return {
        "status": "healthy",
        "service": "baseline_single_node_rag",
        "vector_count": vector_store.count(),
        "embedding_model": config.EMBEDDING_MODEL_NAME,
        "llm_provider": config.LLM_PROVIDER
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

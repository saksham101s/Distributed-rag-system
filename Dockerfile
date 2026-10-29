# Two targets from one file:
#   docker build --target shard .        -> slim gRPC FAISS shard (no torch / embedding model)
#   docker build --target coordinator .  -> FastAPI coordinator with the embedding model and the built UI baked in

FROM node:20-slim AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.11-slim AS base
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
COPY requirements-shard.txt .
RUN pip install -r requirements-shard.txt

FROM base AS shard
COPY proto_gen/ proto_gen/
COPY shared/ shared/
COPY shard_node/ shard_node/
EXPOSE 50051
CMD ["python", "shard_node/shard_service.py", "--shard-id", "shard_0", "--port", "50051"]

FROM base AS coordinator
# CPU-only torch keeps the image ~1.5GB smaller than the default CUDA wheel
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu
COPY requirements.txt .
RUN pip install -r requirements.txt
ARG EMBEDDING_MODEL_NAME=all-MiniLM-L6-v2
ENV EMBEDDING_MODEL_NAME=${EMBEDDING_MODEL_NAME}
# Pre-download the embedding model so the first request doesn't wait on a ~90MB download
RUN python -c "import os; from sentence_transformers import SentenceTransformer; SentenceTransformer(os.environ['EMBEDDING_MODEL_NAME'])"
COPY . .
COPY --from=ui /ui/dist frontend/dist
EXPOSE 8000
CMD ["python", "coordinator/coordinator_service.py"]

import logging
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger("Telemetry")

# Prometheus Metrics Definition
RAG_QUERY_LATENCY = Histogram(
    "rag_query_latency_seconds",
    "End-to-end RAG query execution latency in seconds",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

RAG_CACHE_HITS = Counter(
    "rag_cache_hits_total",
    "Total count of RAG query cache hits",
    ["status"]  # "hit" or "miss"
)

RAG_SHARD_LATENCY = Histogram(
    "rag_shard_request_duration_seconds",
    "gRPC scatter-gather latency per shard in seconds",
    ["shard_id", "status"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0)
)

RAG_NODE_HEALTH = Gauge(
    "rag_node_health_status",
    "Health status per shard node (1 for healthy, 0 for unreachable)",
    ["shard_id"]
)

RAG_DOCUMENTS_INGESTED = Counter(
    "rag_documents_ingested_total",
    "Total count of ingested documents into cluster"
)


def setup_telemetry():
    """Initializes OpenTelemetry tracing context."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        provider = TracerProvider()
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        logger.info("OpenTelemetry tracing provider initialized.")
    except Exception as e:
        logger.warning(f"OpenTelemetry initialization notice: {e}")

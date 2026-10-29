import logging
from contextlib import contextmanager
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

RAG_REPAIR_COPIES = Counter(
    "rag_repair_operations_total",
    "Documents copied or trimmed by the anti-entropy repair loop",
    ["action"]  # "copied" or "trimmed"
)


def setup_telemetry(exporter: str = "none"):
    """Installs the OpenTelemetry tracer provider. exporter: "none" (spans are created, not exported) or "console"."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        provider = TracerProvider()
        if exporter == "console":
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing provider initialized (exporter={exporter}).")
    except Exception as e:
        logger.warning(f"OpenTelemetry initialization notice: {e}")


def instrument_fastapi(app):
    """Adds server spans for every REST request."""
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app, excluded_urls="metrics")
    except Exception as e:
        logger.warning(f"FastAPI instrumentation unavailable: {e}")


def instrument_grpc_client():
    """Adds client spans to every grpc.aio call and propagates trace context to shards."""
    try:
        from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorClient
        GrpcAioInstrumentorClient().instrument()
    except Exception as e:
        logger.warning(f"gRPC client instrumentation unavailable: {e}")


def instrument_grpc_server():
    """Adds server spans to every grpc.aio servicer call."""
    try:
        from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorServer
        GrpcAioInstrumentorServer().instrument()
    except Exception as e:
        logger.warning(f"gRPC server instrumentation unavailable: {e}")


@contextmanager
def span(name: str, **attributes):
    """Opens a manual span (e.g. around embedding or LLM calls). No-op if OpenTelemetry is missing."""
    try:
        from opentelemetry import trace
    except Exception:
        yield None
        return
    with trace.get_tracer("rag").start_as_current_span(name) as s:
        for key, value in attributes.items():
            s.set_attribute(key, value)
        yield s

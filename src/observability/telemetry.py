import logging
import os
import structlog
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from dotenv import load_dotenv

load_dotenv()


def setup_telemetry():
    """Initialize OpenTelemetry tracing and metrics."""
    resource = Resource.create({
        "service.name": os.getenv("OTEL_SERVICE_NAME", "etl-pipeline"),
        "service.version": os.getenv("DD_VERSION", "1.0.0"),
        "deployment.environment": os.getenv("DD_ENV", "local"),
    })

    # --- Tracing ---
    otlp_trace_exporter = OTLPSpanExporter(
        endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
        insecure=True,
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(otlp_trace_exporter))
    trace.set_tracer_provider(tracer_provider)

    # --- Metrics ---
    otlp_metric_exporter = OTLPMetricExporter(
        endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
        insecure=True,
    )
    metric_reader = PeriodicExportingMetricReader(otlp_metric_exporter, export_interval_millis=5000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    # --- Auto-instrumentation ---
    RequestsInstrumentor().instrument()

    # --- Structured logging ---
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
    )

    return trace.get_tracer(__name__), metrics.get_meter(__name__)


tracer, meter = setup_telemetry()

# Custom metrics
records_ingested_counter = meter.create_counter(
    "etl.records.ingested",
    description="Number of records ingested from source",
)
records_processed_counter = meter.create_counter(
    "etl.records.processed",
    description="Number of records processed/transformed",
)
records_sinked_counter = meter.create_counter(
    "etl.records.sinked",
    description="Number of records written to sink",
)
pipeline_duration_histogram = meter.create_histogram(
    "etl.pipeline.duration_ms",
    description="ETL pipeline execution duration in ms",
    unit="ms",
)
pipeline_errors_counter = meter.create_counter(
    "etl.pipeline.errors",
    description="Number of pipeline errors",
)
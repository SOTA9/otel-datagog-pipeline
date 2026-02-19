import os
import time
import traceback
import structlog
from dotenv import load_dotenv
from opentelemetry import trace
from src.observability.telemetry import tracer, pipeline_duration_histogram, pipeline_errors_counter
from src.ingestion.data_source import DataSource
from src.processing.transformer import DataTransformer
from src.sink.postgres_sink import PostgresSink

load_dotenv()
log = structlog.get_logger()


def run_pipeline():
    start = time.time()
    with tracer.start_as_current_span("etl_pipeline") as span:
        span.set_attribute("pipeline.name", "etl-observability-pipeline")
        span.set_attribute("pipeline.env", os.getenv("DD_ENV", "local"))
        try:
            log.info("pipeline_started")

            source = DataSource(
                source_url=os.getenv("SOURCE_API_URL"),
                use_synthetic=os.getenv("USE_SYNTHETIC", "true").lower() == "true",
            )
            raw_data = source.fetch(batch_size=int(os.getenv("BATCH_SIZE", 50)))

            transformer = DataTransformer()
            df = transformer.transform(raw_data)

            sink = PostgresSink()
            sink.write(df)

            duration_ms = (time.time() - start) * 1000
            pipeline_duration_histogram.record(duration_ms, {"status": "success"})
            log.info("pipeline_completed", duration_ms=round(duration_ms, 2), records=len(df))

        except Exception as e:
            pipeline_errors_counter.add(1, {"stage": "pipeline"})
            span.set_status(trace.StatusCode.ERROR, str(e))
            log.error("pipeline_failed", error=str(e))
            traceback.print_exc()
            raise


if __name__ == "__main__":
    run_pipeline()
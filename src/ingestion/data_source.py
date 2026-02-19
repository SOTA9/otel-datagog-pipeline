import requests
import structlog
from faker import Faker
from opentelemetry import trace
from src.observability.telemetry import tracer, records_ingested_counter

log = structlog.get_logger()
fake = Faker()


class DataSource:
    """Ingests data from an external API or generates synthetic data."""

    def __init__(self, source_url: str = None, use_synthetic: bool = False):
        self.source_url = source_url
        self.use_synthetic = use_synthetic

    def fetch(self, batch_size: int = 100) -> list[dict]:
        with tracer.start_as_current_span("data_ingestion") as span:
            span.set_attribute("source.url", str(self.source_url))
            span.set_attribute("source.type", "synthetic" if self.use_synthetic else "api")

            try:
                if self.use_synthetic:
                    data = self._generate_synthetic(batch_size)
                else:
                    data = self._fetch_from_api()

                records_ingested_counter.add(len(data), {"source": "api"})
                span.set_attribute("records.count", len(data))
                log.info("data_ingested", count=len(data))
                return data

            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR, str(e))
                log.error("ingestion_failed", error=str(e))
                raise

    def _fetch_from_api(self) -> list[dict]:
        response = requests.get(self.source_url, timeout=10)
        response.raise_for_status()
        return response.json()

    def _generate_synthetic(self, count: int) -> list[dict]:
        return [
            {
                "name": fake.name(),
                "email": fake.email(),
                "company": fake.company(),
                "city": fake.city(),
                "country": fake.country(),
                "phone": fake.phone_number(),
                "created_at": fake.date_time_this_year().isoformat(),
            }
            for _ in range(count)
        ]
import pandas as pd
import structlog
from opentelemetry import trace
from src.observability.telemetry import tracer, records_processed_counter

log = structlog.get_logger()


class DataTransformer:

    def transform(self, records: list) -> pd.DataFrame:
        with tracer.start_as_current_span("data_processing") as span:
            span.set_attribute("input.records", len(records))
            try:
                df = pd.DataFrame(records)
                df = self._clean(df)
                df = self._enrich(df)
                records_processed_counter.add(len(df), {"stage": "transform"})
                span.set_attribute("output.records", len(df))
                log.info("data_transformed", input=len(records), output=len(df))
                return df
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR, str(e))
                log.error("transform_failed", error=str(e))
                raise

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df = df.drop_duplicates()
        if "email" in df.columns:
            df = df.dropna(subset=["email"])
            df["email"] = df["email"].str.lower().str.strip()
        if "name" in df.columns:
            df["name"] = df["name"].str.strip().str.title()
        return df

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df["processed_at"] = pd.Timestamp.utcnow()
        df["pipeline_version"] = "1.0.0"
        return df
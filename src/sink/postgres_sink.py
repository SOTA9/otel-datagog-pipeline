import os
import structlog
from sqlalchemy import create_engine, text
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry import trace
import pandas as pd
from src.observability.telemetry import tracer, records_sinked_counter

log = structlog.get_logger()


class PostgresSink:

    def __init__(self):
        db_url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('POSTGRES_USER', 'etl_user')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'etl_password')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'etl_db')}"
        )
        self.engine = create_engine(db_url, echo=False)
        SQLAlchemyInstrumentor().instrument(engine=self.engine)
        self._ensure_table()

    def _ensure_table(self):
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_records (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    company TEXT,
                    city TEXT,
                    country TEXT,
                    phone TEXT,
                    created_at TEXT,
                    processed_at TIMESTAMPTZ,
                    pipeline_version TEXT
                );
            """))
            conn.commit()

    def write(self, df: pd.DataFrame, table: str = "etl_records"):
        with tracer.start_as_current_span("data_sink") as span:
            span.set_attribute("sink.table", table)
            span.set_attribute("sink.records", len(df))
            try:
                # Drop id column — let Postgres auto-generate it
                if "id" in df.columns:
                    df = df.drop(columns=["id"])
                df.to_sql(table, self.engine, if_exists="append", index=False, method="multi")
                records_sinked_counter.add(len(df), {"table": table})
                log.info("data_sinked", table=table, count=len(df))
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR, str(e))
                log.error("sink_failed", error=str(e))
                raise
# ETL Observability Pipeline

A complete end-to-end ETL pipeline with full observability using OpenTelemetry, Grafana, Prometheus, and Datadog. Built and tested locally using PyCharm Community Edition.

---

## Prerequisites

- Python 3.11+
- PyCharm Community Edition
- Docker Desktop
- Git
- A Datadog account (EU: datadoghq.eu)

---

## Environment Setup

### 1. Clone or create the project

```bash
mkdir etl-observability-pipeline
cd etl-observability-pipeline
```

### 2. Create and activate virtual environment

```bash
python -m venv venv

# Windows (PowerShell)
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
pip install pyarrow
```

### 4. Configure environment variables

Copy `.env.example` to `.env`:

```bash
# Windows
copy .env.example .env

# Mac/Linux
cp .env.example .env
```

Edit `.env` with your values:
---

### `docker/prometheus.yml`

```yaml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: "otel-collector"
    static_configs:
      - targets: ["otel-collector:8889"]
```

---

## Start Infrastructure

```bash
cd docker
docker-compose up -d

# Verify all 4 containers are running
docker-compose ps
```

Expected containers running:

| Container | Port |
|---|---|
| docker-postgres-1 | 5432 |
| docker-otel-collector-1 | 4317, 4318, 8888, 8889 |
| docker-prometheus-1 | 9090 |
| docker-grafana-1 | 3000 |
| docker-datadog-agent-1 | 4315 |

---
## Run the Pipeline

From the project root:

```bash
python -m src.pipeline
```

Expected output (structured JSON logs):

```json
{"event": "pipeline_started", "level": "info", "timestamp": "..."}
{"count": 100, "event": "data_ingested", "level": "info", "timestamp": "..."}
{"input": 100, "output": 100, "event": "data_transformed", "level": "info", "timestamp": "..."}
{"table": "etl_records", "count": 100, "event": "data_sinked", "level": "info", "timestamp": "..."}
{"duration_ms": 656.3, "records": 100, "event": "pipeline_completed", "level": "info", "timestamp": "..."}
```

Run multiple times to generate metric volume:

```bash
python -m src.pipeline
python -m src.pipeline
python -m src.pipeline
python -m src.pipeline
python -m src.pipeline
```

---

## How the Pipeline Works

```
Faker (synthetic data)
        ↓
   DataSource.fetch()          → OTel span: data_ingestion
        ↓
   DataTransformer.transform() → OTel span: data_processing
        ↓
   PostgresSink.write()        → OTel span: data_sink
        ↓
   PostgreSQL etl_records table
        ↓
   OTel Collector (port 4317)
        ↓         ↓
  Prometheus    Datadog Agent
        ↓
     Grafana
```

---

## Observability URLs

| Tool | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | none |
| OTel metrics | http://localhost:8889/metrics | none |
| Datadog APM | https://app.datadoghq.eu/apm/traces | your account |
| Datadog Metrics | https://app.datadoghq.eu/metric/explorer | your account |

---

## Prometheus Queries

Go to http://localhost:9090 and run:

```
etl_records_ingested_total
etl_records_processed_total
etl_records_sinked_total
etl_pipeline_duration_ms_sum / etl_pipeline_duration_ms_count
etl_pipeline_errors_total
```

---

## Grafana Dashboard Setup

1. Go to http://localhost:3000 → login `admin / admin`
2. **Connections** → **Data Sources** → **Add data source** → **Prometheus**
3. URL: `http://prometheus:9090` → **Save & Test**
4. **Dashboards** → **New** → **New Dashboard** → **Add visualization**
5. Add panels with these queries:

| Panel | Query |
|---|---|
| Records Ingested | `etl_records_ingested_total` |
| Records Sinked | `etl_records_sinked_total` |
| Pipeline Duration | `etl_pipeline_duration_ms_sum / etl_pipeline_duration_ms_count` |
| Errors | `etl_pipeline_errors_total` |

---

## Verify Data in Postgres

```bash
# Row count
docker exec -it docker-postgres-1 psql -U etl_user -d etl_db -c "SELECT COUNT(*) FROM etl_records;"

# Preview data
docker exec -it docker-postgres-1 psql -U etl_user -d etl_db -c "SELECT name, email, processed_at FROM etl_records LIMIT 5;"
```

---

## Run Tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=src --cov-report=html
```

---

## Troubleshooting

### Docker containers not starting
```bash
docker-compose down -v
docker volume prune -f
docker-compose up -d
docker-compose ps
```

### Postgres connection error
Verify your `.env` has correct credentials and that the `docker-postgres-1` container is healthy:
```bash
docker ps | findstr postgres
```

### Prometheus shows no data
Check OTel Collector is running and scrape target is UP at http://localhost:9090/targets. Run pipeline at least 3 times and wait 30 seconds.

### Grafana not opening
Check port 3000 is free:
```bash
# Windows
netstat -ano | findstr :3000
```
If occupied, change Grafana port to `3001:3000` in docker-compose.yaml.

### Datadog shows no data
- Verify API key is correct at https://app.datadoghq.eu/organization-settings/api-keys
- Check agent logs: `docker logs docker-datadog-agent-1 --tail 20`
- Look for `Successfully validated API key` in the logs
- Make sure `DD_SITE=datadoghq.eu` (not `.com`) in your config

---

## Push to GitHub

```bash
git init
git add .
git commit -m "feat: ETL pipeline with OpenTelemetry, Grafana, Prometheus, Datadog observability"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/etl-observability-pipeline.git
git push -u origin main
```

---

## Custom Metrics Tracked

| Metric | Type | Description |
|---|---|---|
| `etl.records.ingested` | Counter | Records fetched from source |
| `etl.records.processed` | Counter | Records after transformation |
| `etl.records.sinked` | Counter | Records written to Postgres |
| `etl.pipeline.duration_ms` | Histogram | End-to-end pipeline duration |
| `etl.pipeline.errors` | Counter | Pipeline failures count |

---

## OTel Spans per Pipeline Run

| Span | Stage | Attributes |
|---|---|---|
| `etl_pipeline` | Full run | pipeline.name, pipeline.env |
| `data_ingestion` | Ingest | source.type, records.count |
| `data_processing` | Transform | input.records, output.records |
| `data_sink` | Sink | sink.table, sink.records |
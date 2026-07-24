# QuakeStream

> When the ground moves, minutes matter. QuakeStream turns the live USGS earthquake
> feed into insight: a fully containerized streaming pipeline that ingests seismic
> events, buffers them through Kafka, processes them in real time with Spark
> Structured Streaming, and lands them as versioned Delta tables on object storage.
> Analysts query the results instantly with Trino and watch them unfold on a live
> Streamlit dashboard — the entire lakehouse spun up with a single `docker compose up`.

---

## Stack
`Python producer → Kafka (KRaft) → Spark Structured Streaming → Delta on MinIO → Trino → Streamlit`

All eight services run in Docker on one network.

## Prerequisites
- Docker Desktop (memory raised to 8 GB+)
- Git

## Quickstart
```bash
cp .env.example .env
docker compose up --build
```

## Build phases
- Phase 0 — Scaffold
- Phase 1 — Kafka
- Phase 2 — Producer
- Phase 3 — MinIO
- Phase 4 — Spark
- Phase 5 — Trino + metastore
- Phase 6 — Streamlit
- Phase 7 — Wire together

---

# Architecture

| Layer | Technology | Purpose |
|--------|------------|---------|
| **Connect** | FastAPI + Python | Polls a JSON API and publishes NDJSON events to Kafka |
| **Buffer** | Kafka + Zookeeper | Event streaming using the `events` topic |
| **Process** | Spark Structured Streaming | Reads Kafka events, parses JSON, transforms data, and writes Delta Lake tables |
| **Storage** | MinIO | S3-compatible object storage containing Delta Lake files (Parquet + `_delta_log`) |
| **Query Engine** | Trino + Hive Metastore | Enables SQL queries on Delta tables stored in MinIO |
| **Visualization** | Streamlit | Interactive dashboard querying Trino via `trino-python-client` |

---

# Docker Compose Services

| Service | Description |
|----------|-------------|
| **zookeeper** | Coordination service required by Kafka |
| **kafka** | Kafka broker hosting the `events` topic |
| **producer** | FastAPI application that polls a JSON source (e.g., USGS Earthquake API) and publishes events to Kafka |
| **spark** | Spark Structured Streaming job that consumes Kafka events, transforms them, and writes Delta Lake tables to MinIO |
| **minio** | S3-compatible object storage used as the Delta Lake backend |
| **hive-metastore** | Metadata catalog used by Trino to discover Delta tables |
| **metastore-db** | PostgreSQL/MariaDB database backing the Hive Metastore |
| **trino** | Distributed SQL query engine with the Delta Lake connector configured for MinIO and the Hive Metastore |
| **streamlit** | Dashboard application that queries Trino and displays analytics |

---
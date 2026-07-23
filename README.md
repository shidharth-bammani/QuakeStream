# QuakeStream

> When the ground moves, minutes matter. QuakeStream turns the live USGS earthquake
> feed into insight: a fully containerized streaming pipeline that ingests seismic
> events, buffers them through Kafka, processes them in real time with Spark
> Structured Streaming, and lands them as versioned Delta tables on object storage.
> Analysts query the results instantly with Trino and watch them unfold on a live
> Streamlit dashboard — the entire lakehouse spun up with a single `docker compose up`.

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
- [x] Phase 0 — Scaffold
- [ ] Phase 1 — Kafka
- [ ] Phase 2 — Producer
- [ ] Phase 3 — MinIO
- [ ] Phase 4 — Spark
- [ ] Phase 5 — Trino + metastore
- [ ] Phase 6 — Streamlit
- [ ] Phase 7 — Wire together
- [ ] Phase 8 — Polish
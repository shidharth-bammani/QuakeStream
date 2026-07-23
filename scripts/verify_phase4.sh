#!/usr/bin/env bash
# Phase 4 check: build + start Spark, watch it land Delta files in MinIO.
set -e
echo "==> Building & starting Spark (first run downloads jars, be patient)"
docker compose up -d --build spark
echo "==> Following Spark logs — look for 'streaming Kafka ... -> Delta'"
echo "    (Ctrl+C to stop watching; container keeps running)"
docker compose logs -f spark

#!/usr/bin/env bash
# Phase 1 smoke test: bring up Kafka, create topic, round-trip a message.
set -e

echo "==> Starting Kafka + topic init"
docker compose up -d kafka
docker compose up kafka-init        # runs once, prints topic list

echo "==> Producing a test message"
echo '{"test":"hello quakestream"}' | \
  docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic events

echo "==> Consuming it back (Ctrl-C to stop)"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic events --from-beginning --timeout-ms 5000

echo "==> Phase 1 OK"
"""QuakeStream producer — polls the USGS earthquake feed and publishes
each event as a flat JSON record to Kafka.

Dedupe strategy: USGS re-lists the same quakes on every poll. We key each
Kafka message by the USGS event id and skip re-sending unless the event's
`updated` timestamp changed (quakes get revised as data comes in).
"""
import json
import os
import sys
import time
import signal
import logging

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger("producer")

FEED_URL = os.environ["USGS_FEED_URL"]
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
POLL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

# id -> last seen `updated` timestamp, so we only publish new/changed quakes
_seen: dict[str, int] = {}
_running = True


def _stop(signum, frame):
    global _running
    log.info("Shutdown signal received, stopping after this cycle.")
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def connect_producer() -> KafkaProducer:
    """Retry until the Kafka broker is reachable (it may still be starting)."""
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=100,
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP)
            return p
        except NoBrokersAvailable:
            log.warning("Kafka not reachable yet, retrying in 3s...")
            time.sleep(3)


def flatten(feature: dict) -> dict:
    """Turn a GeoJSON feature into one flat record."""
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}
    coords = geom.get("coordinates", [None, None, None]) or [None, None, None]
    lon, lat, depth = (coords + [None, None, None])[:3]
    return {
        "id": feature.get("id"),
        "mag": props.get("mag"),
        "place": props.get("place"),
        "time": props.get("time"),          # epoch millis
        "updated": props.get("updated"),    # epoch millis
        "tsunami": props.get("tsunami"),
        "sig": props.get("sig"),            # significance score
        "magType": props.get("magType"),
        "type": props.get("type"),
        "title": props.get("title"),
        "url": props.get("url"),
        "longitude": lon,
        "latitude": lat,
        "depth_km": depth,
    }


def poll_once(producer: KafkaProducer) -> int:
    resp = requests.get(FEED_URL, timeout=30)
    resp.raise_for_status()
    features = resp.json().get("features", [])
    new_count = 0
    for f in features:
        eid = f.get("id")
        updated = (f.get("properties") or {}).get("updated", 0)
        if eid is None:
            continue
        if _seen.get(eid) == updated:
            continue  # unchanged since last time
        record = flatten(f)
        producer.send(TOPIC, key=eid, value=record)
        _seen[eid] = updated
        new_count += 1
    producer.flush()
    return new_count


def main():
    log.info("Feed: %s", FEED_URL)
    log.info("Topic: %s   Poll interval: %ss", TOPIC, POLL_SECONDS)
    producer = connect_producer()

    while _running:
        try:
            n = poll_once(producer)
            log.info("Cycle done: %d new/updated events published (tracking %d ids)",
                     n, len(_seen))
        except requests.RequestException as e:
            log.error("Feed fetch failed: %s", e)
        except Exception as e:  # keep the loop alive
            log.exception("Unexpected error: %s", e)

        # sleep in 1s slices so shutdown is responsive
        for _ in range(POLL_SECONDS):
            if not _running:
                break
            time.sleep(1)

    producer.close()
    log.info("Producer stopped cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()
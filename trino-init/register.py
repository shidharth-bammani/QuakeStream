"""One-shot: register the Spark-written Delta table in Trino's metastore.

Waits for Trino to be ready, creates the schema, then calls
system.register_table (idempotent — ignores 'already exists').
"""
import os
import time
import sys
import trino

HOST = os.environ.get("TRINO_HOST", "trino")
PORT = int(os.environ.get("TRINO_PORT", "8080"))
CATALOG = os.environ.get("TRINO_CATALOG", "delta")
SCHEMA = os.environ.get("TRINO_SCHEMA", "default")
BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")

SCHEMA_LOC = f"s3://{BUCKET}/"
TABLE_LOC = f"s3://{BUCKET}/quakes"


def conn():
    return trino.dbapi.connect(host=HOST, port=PORT, user="init", catalog=CATALOG)


def wait_for_trino(timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            c = conn().cursor()
            c.execute("SELECT 1")
            c.fetchall()
            print("Trino is ready.")
            return
        except Exception as e:
            print(f"Waiting for Trino... ({e.__class__.__name__})")
            time.sleep(5)
    print("Timed out waiting for Trino.", file=sys.stderr)
    sys.exit(1)


def run(sql, ignore_substr=None):
    try:
        cur = conn().cursor()
        cur.execute(sql)
        cur.fetchall()
        print(f"OK: {sql[:70]}")
    except Exception as e:
        if ignore_substr and ignore_substr.lower() in str(e).lower():
            print(f"Skip (already exists): {sql[:50]}")
        else:
            raise


def main():
    wait_for_trino()
    run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
        f"WITH (location = '{SCHEMA_LOC}')")
    run(f"CALL {CATALOG}.system.register_table("
        f"schema_name => '{SCHEMA}', table_name => 'quakes', "
        f"table_location => '{TABLE_LOC}')",
        ignore_substr="already")
    # sanity read
    cur = conn().cursor()
    cur.execute(f"SELECT count(*) FROM {CATALOG}.{SCHEMA}.quakes")
    print(f"Row count now: {cur.fetchone()[0]}")
    print("Registration complete.")


if __name__ == "__main__":
    main()

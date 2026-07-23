"""QuakeStream dashboard — live view of the Delta table via Trino."""
import os
import time
import pandas as pd
import streamlit as st
import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
CATALOG = os.environ.get("TRINO_CATALOG", "delta")
SCHEMA = os.environ.get("TRINO_SCHEMA", "default")
TABLE = f"{CATALOG}.{SCHEMA}.quakes"

st.set_page_config(page_title="QuakeStream", page_icon="🌎", layout="wide")


def query(sql: str) -> pd.DataFrame:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="dashboard",
                               catalog=CATALOG, schema=SCHEMA)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


st.title("🌎 QuakeStream — live seismic activity")
st.caption(f"Streaming USGS earthquakes · source table `{TABLE}`")

# sidebar controls
with st.sidebar:
    st.header("Controls")
    min_mag = st.slider("Minimum magnitude", 0.0, 8.0, 0.0, 0.5)
    auto = st.checkbox("Auto-refresh (10s)", value=True)
    if st.button("Refresh now"):
        st.rerun()

try:
    # headline metrics
    m = query(f"""
        SELECT count(*) AS total,
               round(max(mag), 1) AS max_mag,
               round(avg(mag), 2) AS avg_mag,
               sum(tsunami) AS tsunami_flags
        FROM {TABLE}
        WHERE mag IS NOT NULL AND mag >= {min_mag}
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Quakes", int(m["total"]))
    c2.metric("Strongest", m["max_mag"])
    c3.metric("Average mag", m["avg_mag"])
    c4.metric("Tsunami flags", int(m["tsunami_flags"] or 0))

    # data for map + table
    df = query(f"""
        SELECT id, place, mag, depth_km, latitude, longitude,
               from_unixtime(time/1000) AS when_utc
        FROM {TABLE}
        WHERE mag IS NOT NULL AND mag >= {min_mag}
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY time DESC
    """)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Map")
        if not df.empty:
            st.map(df[["latitude", "longitude"]].rename(
                columns={"latitude": "lat", "longitude": "lon"}))
        else:
            st.info("No quakes match the current filter yet.")
    with right:
        st.subheader("Recent events")
        st.dataframe(
            df[["when_utc", "place", "mag", "depth_km"]].head(25),
            use_container_width=True, hide_index=True,
        )

except Exception as e:
    st.error(f"Could not query Trino yet: {e}")
    st.info("If the stack just started, give Trino a few seconds and refresh.")

if auto:
    time.sleep(10)
    st.rerun()

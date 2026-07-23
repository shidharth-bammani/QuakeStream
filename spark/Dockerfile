FROM apache/spark:3.5.3

# Pinned dependency versions — MUST match the Spark/Scala build.
# Spark 3.5.3 = Scala 2.12, bundled Hadoop 3.3.4.
ENV DELTA_VERSION=3.2.1 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262 \
    SPARK_KAFKA_VERSION=3.5.3

USER root

# Delta python API (optional but handy). requests not needed here.
RUN pip install --no-cache-dir delta-spark==${DELTA_VERSION}

COPY jobs/ /opt/spark/jobs/

# ivy cache dir must be writable by the spark user (uid 185)
RUN mkdir -p /opt/spark/.ivy2 && chown -R 185:185 /opt/spark/.ivy2 /opt/spark/jobs

USER 185

ENV PYTHONUNBUFFERED=1

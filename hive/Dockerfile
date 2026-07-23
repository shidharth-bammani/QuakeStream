FROM apache/hive:4.0.0

USER root

# Postgres JDBC driver — metastore backend
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql.jar

# S3A filesystem so the metastore can resolve s3a:// table locations.
# Hive 4.0.0 bundles Hadoop 3.3.6, so match hadoop-aws to 3.3.6.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle.jar

RUN chmod 644 /opt/hive/lib/postgresql.jar /opt/hive/lib/hadoop-aws.jar /opt/hive/lib/aws-java-sdk-bundle.jar

USER hive

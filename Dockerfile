# Builds the jars inside Docker so the demo needs nothing but Docker itself.
#
# The requirements ask for the demo to run locally on Docker with Kafka and
# Flink. "Did you build first?" is not a step anyone should have to remember in
# front of an audience, so the build happens here rather than on the host.

# ---------------------------------------------------------------- build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src

# Dependencies resolve in their own layer, so editing a source file does not
# re-download the world.
COPY pom.xml ./
COPY common/pom.xml common/
COPY generators/pom.xml generators/
COPY jobs/pom.xml jobs/
RUN mvn -B -q dependency:go-offline -DexcludeArtifactIds=flink-training-common 2>/dev/null || true

COPY common/src common/src
COPY generators/src generators/src
COPY jobs/src jobs/src
RUN mvn -B -q package -DskipTests -DskipITs

# ---------------------------------------------------------------- generators
FROM eclipse-temurin:17-jre AS generators
COPY --from=build /src/generators/target/generators.jar /app/generators.jar
COPY docker/generators/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]

# ---------------------------------------------------------------- jobs
# Carries the jar and the Flink client, so it can submit and then exit.
FROM flink:1.20.4-java17 AS jobs
COPY --from=build /src/jobs/target/jobs.jar /opt/jobs/jobs.jar


# The broker with a JMX exporter attached, so Prometheus can see it.
#
# Kafka publishes everything worth knowing over JMX and nothing over HTTP, which
# is why step 10 diagnosed a broker-bound pipeline by running `docker stats` by
# hand. The agent turns JMX into a scrape endpoint: bytes in and out, request
# handler idle time, and log size on disk.
FROM apache/kafka:3.9.2 AS kafka
ARG JMX_AGENT_VERSION=1.0.1
USER root
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY docker/kafka/jmx-exporter.yml /opt/jmx/jmx-exporter.yml
RUN chmod 644 /opt/jmx/jmx_prometheus_javaagent.jar /opt/jmx/jmx-exporter.yml
USER appuser

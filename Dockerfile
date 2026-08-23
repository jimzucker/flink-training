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

# Build:  docker build -t flink2-check .
# Run:    docker run --rm -v "$PWD:/work" flink2-check /work
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline -DexcludeScope=test || true
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
COPY --from=build /src/target/flink2-check.jar /opt/flink2-check/flink2-check.jar
WORKDIR /work
ENTRYPOINT ["java", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "-jar", "/opt/flink2-check/flink2-check.jar"]
CMD ["--help"]

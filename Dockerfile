# Stage 1: Build (Gradle pre-installed)
FROM gradle:8.14-jdk21-alpine AS builder

WORKDIR /app

# Copy gradle files first for dependency caching
COPY build.gradle.kts settings.gradle.kts gradle.properties ./

# Download dependencies (cached layer)
RUN gradle dependencies --no-daemon

# Copy source
COPY src src

# Build
RUN gradle build -x test --no-daemon

# Stage 2: Runtime (~90MB)
FROM bellsoft/liberica-openjre-alpine:21

WORKDIR /app

# Copy the Quarkus app
COPY --from=builder /app/build/quarkus-app/lib/ /app/lib/
COPY --from=builder /app/build/quarkus-app/*.jar /app/
COPY --from=builder /app/build/quarkus-app/app/ /app/app/
COPY --from=builder /app/build/quarkus-app/quarkus/ /app/quarkus/

ENV JAVA_OPTS="-Djava.util.logging.manager=org.jboss.logmanager.LogManager"

EXPOSE 8080

ENTRYPOINT ["java", "-Djava.util.logging.manager=org.jboss.logmanager.LogManager", "-jar", "/app/quarkus-run.jar"]

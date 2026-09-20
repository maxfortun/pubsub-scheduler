# Stage 1: Build (Gradle pre-installed)
FROM gradle:8.14-jdk21-alpine AS builder

WORKDIR /app

# Install custom CA certificates (for corporate proxies like Zscaler)
# Place .crt/.pem files in certs/ directory
COPY certs/ /tmp/certs/
RUN set -e; \
    JAVA_CACERTS="${JAVA_HOME}/lib/security/cacerts"; \
    for cert in /tmp/certs/*.crt /tmp/certs/*.pem /tmp/certs/*.cer; do \
      if [ -f "$cert" ]; then \
        echo "Installing certificate: $cert"; \
        cp "$cert" /usr/local/share/ca-certificates/ 2>/dev/null || true; \
        keytool -import -trustcacerts -keystore "$JAVA_CACERTS" -storepass changeit -noprompt \
          -alias "$(basename $cert .crt)" -file "$cert" || true; \
      fi; \
    done; \
    if command -v update-ca-certificates >/dev/null 2>&1; then \
      update-ca-certificates; \
    fi

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

# JVM memory settings - tuned for Quarkus + Camel + Kafka
# Heap: 512MB default, configurable via JAVA_OPTS_APPEND
# MetaSpace: 128MB for Quarkus/Camel class loading
# Container-aware: uses cgroup limits when available
ENV JAVA_OPTS="-Djava.util.logging.manager=org.jboss.logmanager.LogManager \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=200 \
    -XX:+UseContainerSupport \
    -XX:MaxRAMPercentage=75.0 \
    -XX:InitialRAMPercentage=50.0 \
    -Xms256m \
    -Xmx512m \
    -XX:MaxMetaspaceSize=128m"

# Allow additional JVM options via environment
ENV JAVA_OPTS_APPEND=""

EXPOSE 8080

# Use shell form to allow variable expansion
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS $JAVA_OPTS_APPEND -jar /app/quarkus-run.jar"]

# Stage 1: Build (Gradle pre-installed)
FROM gradle:8.14-jdk21-alpine AS builder

WORKDIR /app

# Install custom CA certificates (for corporate proxies like Zscaler)
# Place .crt/.pem files in certs/ directory
COPY certs/ /tmp/certs/
RUN if ls /tmp/certs/*.crt /tmp/certs/*.pem /tmp/certs/*.cer 2>/dev/null; then \
      cp /tmp/certs/*.crt /tmp/certs/*.pem /tmp/certs/*.cer /usr/local/share/ca-certificates/ 2>/dev/null || true; \
      update-ca-certificates; \
      # Also add to Java truststore
      for cert in /tmp/certs/*.crt /tmp/certs/*.pem /tmp/certs/*.cer; do \
        if [ -f "$cert" ]; then \
          keytool -import -trustcacerts -cacerts -storepass changeit -noprompt \
            -alias "$(basename $cert)" -file "$cert" 2>/dev/null || true; \
        fi; \
      done; \
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

ENV JAVA_OPTS="-Djava.util.logging.manager=org.jboss.logmanager.LogManager"

EXPOSE 8080

ENTRYPOINT ["java", "-Djava.util.logging.manager=org.jboss.logmanager.LogManager", "-jar", "/app/quarkus-run.jar"]

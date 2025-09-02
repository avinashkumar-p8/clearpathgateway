# Use OpenJDK 21 slim image
FROM openjdk:21-jre-slim

# Set working directory
WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy the JAR file
COPY target/fast-inward-clearing-processor-*.jar app.jar

# Set ownership
RUN chown appuser:appuser /app/app.jar

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health/live || exit 1

# JVM options for production
ENV JAVA_OPTS="--enable-preview \
    -XX:+UseZGC \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseVirtualThreads \
    -Xms512m \
    -Xmx2g \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/tmp \
    -Djava.security.egd=file:/dev/./urandom \
    -Dspring.profiles.active=production"

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
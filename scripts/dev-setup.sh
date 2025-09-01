#!/bin/bash

# Development Environment Setup Script for ClearPath Gateway
set -e

echo "🚀 Setting up ClearPath Gateway development environment..."

# Check prerequisites
check_prerequisites() {
    echo "🔍 Checking prerequisites..."

    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi

    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    # Check Java (for local development)
    if ! command -v java &> /dev/null; then
        echo "⚠️  Java is not installed. You may need Java 17+ for local service development."
    else
        java_version=$(java -version 2>&1 | head -n1 | cut -d'"' -f2)
        echo "✅ Java version: $java_version"
    fi

    # Check kubectl (for K8s deployment)
    if ! command -v kubectl &> /dev/null; then
        echo "⚠️  kubectl is not installed. You may need it for Kubernetes deployment."
    else
        echo "✅ kubectl is available"
    fi

    echo "✅ Prerequisites check completed"
}

# Create necessary directories
create_directories() {
    echo "📁 Creating necessary directories..."

    # Create monitoring config directories
    mkdir -p monitoring/grafana/{dashboards,datasources}
    mkdir -p monitoring/prometheus

    # Create shared library directories
    mkdir -p shared/{common,dto,security,config}

    # Create test directories
    mkdir -p tests/{integration,performance,e2e}

    # Create deployment directories
    mkdir -p deployments/{k8s,terraform,helm}

    # Create log directories
    mkdir -p logs/{services,infrastructure}

    echo "✅ Directories created"
}

# Generate basic configuration files
generate_configs() {
    echo "⚙️  Generating configuration files..."

    # Prometheus configuration
    cat > monitoring/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'fast-router-service'
    static_configs:
      - targets: ['fast-router-service:8080']
    metrics_path: '/actuator/prometheus'

  - job_name: 'fast-inward-clearing-processor'
    static_configs:
      - targets: ['fast-inward-clearing-processor:8080']
    metrics_path: '/actuator/prometheus'

  - job_name: 'fast-outward-clearing-processor'
    static_configs:
      - targets: ['fast-outward-clearing-processor:8080']
    metrics_path: '/actuator/prometheus'

  - job_name: 'fast-sender-service'
    static_configs:
      - targets: ['fast-sender-service:8080']
    metrics_path: '/actuator/prometheus'

  - job_name: 'fast-liquidity-service'
    static_configs:
      - targets: ['fast-liquidity-service:8080']
    metrics_path: '/actuator/prometheus'

  - job_name: 'fast-availability-service'
    static_configs:
      - targets: ['fast-availability-service:8080']
    metrics_path: '/actuator/prometheus'
EOF

    # Grafana datasource configuration
    cat > monitoring/grafana/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

    # Environment template
    cat > .env.template << 'EOF'
# Database Configuration
DATABASE_URL=jdbc:postgresql://postgres:5432/payment_gateway
DATABASE_USERNAME=dev_user
DATABASE_PASSWORD=dev_password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Schema Registry Authentication (required for production Confluent deployments)
# SCHEMA_REGISTRY_USERNAME=your_username
# SCHEMA_REGISTRY_PASSWORD=your_password

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379

# Service URLs (for inter-service communication)
FAST_ROUTER_SERVICE_URL=http://fast-router-service:8080
FAST_INWARD_PROCESSOR_URL=http://fast-inward-clearing-processor:8081
FAST_OUTWARD_PROCESSOR_URL=http://fast-outward-clearing-processor:8082
FAST_SENDER_SERVICE_URL=http://fast-sender-service:8083
FAST_LIQUIDITY_SERVICE_URL=http://fast-liquidity-service:8084
FAST_AVAILABILITY_SERVICE_URL=http://fast-availability-service:8085

# External System URLs (Mock endpoints for development)
G3_HOST_ENDPOINT=http://localhost:8443/g3host
CPG_ENDPOINT=http://localhost:8444/cpg
VAM_ENDPOINT=http://localhost:8445/vam
MIDANZ_ENDPOINT=http://localhost:8446/midanz

# Security Configuration
JWT_SECRET=dev-secret-key-change-in-production
MTLS_ENABLED=false

# SLA Configuration
INWARD_PAYMENT_SLA_SECONDS=4.5
OUTWARD_PAYMENT_SLA_SECONDS=2.0

# Liquidity Configuration
NET_DEBIT_CAP_SGD=500000000.00
REGULATORY_BUFFER_SGD=50000000.00
EOF

    echo "✅ Configuration files generated"
}

# Initialize Kafka topics
initialize_kafka() {
    echo "📝 Initializing Kafka topics..."

    # Wait for Kafka to be ready
    echo "⏳ Waiting for Kafka to be ready..."
    sleep 10

    # Create topics
    topics=(
        "payment-events"
        "payment-messages"
        "liquidity-events"
        "bank-availability"
        "audit-events"
        "exception-queue"
        "dead-letter-queue"
    )

    for topic in "${topics[@]}"; do
        docker-compose exec kafka kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists || echo "Topic $topic may already exist"
    done

    echo "✅ Kafka topics initialized"
}

# Setup database schema
setup_database() {
    echo "🗄️  Setting up database schema..."

    # Wait for PostgreSQL to be ready
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 5

    # Create basic schema (this would normally be done by Flyway/Liquibase)
    docker-compose exec postgres psql -U dev_user -d payment_gateway -c "
    CREATE SCHEMA IF NOT EXISTS payment_gateway;
    CREATE TABLE IF NOT EXISTS payment_gateway.service_health (
        service_name VARCHAR(100) PRIMARY KEY,
        status VARCHAR(20) NOT NULL,
        last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO payment_gateway.service_health (service_name, status)
    VALUES
        ('fast-router-service', 'UNKNOWN'),
        ('fast-inward-clearing-processor', 'UNKNOWN'),
        ('fast-outward-clearing-processor', 'UNKNOWN'),
        ('fast-sender-service', 'UNKNOWN'),
        ('fast-liquidity-service', 'UNKNOWN'),
        ('fast-availability-service', 'UNKNOWN')
    ON CONFLICT (service_name) DO NOTHING;
    " || echo "Database setup completed with warnings"

    echo "✅ Database schema initialized"
}

# Start services
start_services() {
    echo "🚀 Starting services..."

    # Start infrastructure services first
    docker-compose up -d zookeeper kafka redis postgres spanner-emulator schema-registry

    # Wait for infrastructure to be ready
    echo "⏳ Waiting for infrastructure services to be ready..."
    sleep 30

    # Initialize Kafka topics
    initialize_kafka

    # Setup database
    setup_database

    # Start monitoring services
    docker-compose up -d prometheus grafana kafka-ui redis-commander

    echo "✅ Infrastructure and monitoring services started"
    echo ""
    echo "🌐 Available services:"
    echo "   - Schema Registry: http://localhost:8081"
    echo "   - Kafka UI: http://localhost:8090"
    echo "   - Redis Commander: http://localhost:8091"
    echo "   - Prometheus: http://localhost:9090"
    echo "   - Grafana: http://localhost:3000 (admin/admin)"
    echo ""
    echo "🚀 To start the payment gateway services, run:"
    echo "   docker-compose up -d"
    echo ""
    echo "📋 To view logs:"
    echo "   docker-compose logs -f [service-name]"
    echo ""
    echo "🏥 To check service health:"
    echo "   curl http://localhost:8080/health  # Router service"
    echo "   curl http://localhost:8081/health  # Inward processor"
    echo "   curl http://localhost:8082/health  # Outward processor"
    echo "   curl http://localhost:8083/health  # Sender service"
    echo "   curl http://localhost:8084/health  # Liquidity service"
    echo "   curl http://localhost:8085/health  # Availability service"
}

# Main execution
main() {
    echo "🏦 ClearPath Gateway - Development Environment Setup"
    echo "=================================================="

    check_prerequisites
    create_directories
    generate_configs
    start_services

    echo ""
    echo "✅ Development environment setup completed!"
    echo ""
    echo "Next steps:"
    echo "1. Copy .env.template to .env and customize as needed"
    echo "2. Build individual services: cd services/[service-name] && ./mvnw clean package"
    echo "3. Start all services: docker-compose up -d"
    echo "4. Run integration tests: ./scripts/run-tests.sh"
    echo ""
    echo "For more information, see README.md and docs/ARCHITECTURE.md"
}

# Execute main function
main "$@"
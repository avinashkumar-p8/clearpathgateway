#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

print_step() {
    echo -e "${PURPLE}🔧 $1${NC}"
}

# Function to show usage
show_usage() {
    echo -e "${BLUE}🐳 Fast Inward Clearing Processor - Docker Development${NC}"
    echo "================================================================"
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     - Start all services (infrastructure + application)"
    echo "  stop      - Stop all services"
    echo "  restart   - Restart all services"
    echo "  build     - Build the application Docker image"
    echo "  logs      - Show logs for all services"
    echo "  logs-app  - Show logs for application only"
    echo "  status    - Show status of all services"
    echo "  clean     - Clean up everything (containers, images, volumes)"
    echo "  init-db   - Initialize Spanner database only"
    echo "  test      - Run Playwright tests"
    echo "  shell     - Open shell in application container"
    echo "  help      - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start     # Start everything"
    echo "  $0 logs-app  # View application logs"
    echo "  $0 clean     # Clean up everything"
}

# Function to check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_status "Prerequisites check passed"
}

# Function to start infrastructure services
start_infrastructure() {
    print_step "Starting infrastructure services..."
    docker-compose up -d zookeeper kafka schema-registry spanner-emulator
    
    print_info "Waiting for infrastructure services to be ready..."
    sleep 30
    
    # Check service health
    print_info "Checking infrastructure service health..."
    if ! docker-compose ps | grep -q "kafka.*Up"; then
        print_error "Kafka is not running"
        return 1
    fi
    
    if ! docker-compose ps | grep -q "spanner-emulator.*Up"; then
        print_error "Spanner emulator is not running"
        return 1
    fi
    
    print_status "Infrastructure services are running"
}

# Function to initialize Spanner database
init_spanner_database() {
    print_step "Initializing Spanner database..."
    
    # Wait for Spanner emulator to be ready
    print_info "Waiting for Spanner emulator to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:9010 >/dev/null 2>&1; then
            break
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    
    if bash scripts/init-spanner-docker.sh; then
        print_status "Spanner database initialized successfully"
    else
        print_error "Spanner database initialization failed"
        return 1
    fi
}

# Function to build application
build_application() {
    print_step "Building application Docker image..."
    if docker-compose build fast-inward-clearing-processor; then
        print_status "Application built successfully"
    else
        print_error "Application build failed"
        return 1
    fi
}

# Function to start application
start_application() {
    print_step "Starting application..."
    docker-compose up -d fast-inward-clearing-processor
    
    print_info "Waiting for application to be ready..."
    sleep 30
    
    # Check application health
    print_info "Checking application health..."
    if curl -s http://localhost:8080/actuator/health >/dev/null; then
        print_status "Application is running and healthy"
        echo -e "${BLUE}📊 Service URLs:${NC}"
        echo "   Application: http://localhost:8080"
        echo "   Health Check: http://localhost:8080/actuator/health"
        echo "   Metrics: http://localhost:8080/actuator/metrics"
    else
        print_error "Application health check failed"
        return 1
    fi
}

# Main command handling
case "${1:-help}" in
    "start")
        print_header "🚀 Starting Fast Inward Clearing Processor (Docker-Only)"
        echo "================================================================"
        check_prerequisites
        start_infrastructure
        init_spanner_database
        build_application
        start_application
        print_status "🎉 All services started successfully!"
        echo -e "${YELLOW}ℹ️  Use '$0 logs' to view logs${NC}"
        echo -e "${YELLOW}ℹ️  Use '$0 stop' to stop all services${NC}"
        ;;
        
    "stop")
        print_header "🛑 Stopping Fast Inward Clearing Processor"
        echo "================================================================"
        docker-compose down --remove-orphans
        print_status "All services stopped"
        ;;
        
    "restart")
        print_header "🔄 Restarting Fast Inward Clearing Processor"
        echo "================================================================"
        docker-compose restart
        print_status "All services restarted"
        ;;
        
    "build")
        print_header "🔨 Building Application"
        echo "================================================================"
        check_prerequisites
        build_application
        ;;
        
    "logs")
        print_header "📋 Viewing All Service Logs"
        echo "================================================================"
        docker-compose logs -f
        ;;
        
    "logs-app")
        print_header "📋 Viewing Application Logs"
        echo "================================================================"
        docker-compose logs -f fast-inward-clearing-processor
        ;;
        
    "status")
        print_header "📊 Service Status"
        echo "================================================================"
        docker-compose ps
        echo ""
        print_info "Service URLs:"
        echo "   Application: http://localhost:8080"
        echo "   Health Check: http://localhost:8080/actuator/health"
        echo "   Schema Registry: http://localhost:8081"
        echo "   Spanner Emulator: http://localhost:9010"
        ;;
        
    "clean")
        print_header "🧹 Cleaning Up Everything"
        echo "================================================================"
        print_info "Stopping and removing containers..."
        docker-compose down --remove-orphans -v
        print_info "Removing unused images..."
        docker image prune -f
        print_info "Removing unused volumes..."
        docker volume prune -f
        print_status "Cleanup completed"
        ;;
        
    "init-db")
        print_header "🗄️  Initializing Spanner Database"
        echo "================================================================"
        init_spanner_database
        ;;
        
    "test")
        print_header "🧪 Running Playwright Tests"
        echo "================================================================"
        cd playwright
        ./run-tests.sh
        ;;
        
    "shell")
        print_header "🐚 Opening Shell in Application Container"
        echo "================================================================"
        docker-compose exec fast-inward-clearing-processor /bin/bash
        ;;
        
    "help"|*)
        show_usage
        ;;
esac

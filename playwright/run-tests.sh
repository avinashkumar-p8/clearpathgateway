#!/bin/bash

echo "🚀 Starting Kafka + Avro Test Suite"
echo "=================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if dependencies are installed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Check if browsers are installed
if [ ! -d "node_modules/.cache/ms-playwright" ]; then
    echo "🌐 Installing Playwright browsers..."
    npx playwright install
fi

# Start Kafka infrastructure
echo "🐳 Starting Kafka infrastructure..."
docker-compose -f docker-compose-test.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Run tests
echo "🧪 Running tests..."
npm test

# Show results
echo "📊 Test results:"
npm run report

echo "✅ Test suite completed!"

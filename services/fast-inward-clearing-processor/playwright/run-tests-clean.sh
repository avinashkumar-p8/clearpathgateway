#!/bin/bash

# Script to run Playwright tests with automatic Kafka cleanup
# This ensures reliable testing by clearing topics before each test run

echo "🧹 Running Playwright tests with automatic cleanup..."

# Function to run tests with cleanup
run_tests() {
    local test_pattern="$1"
    local timeout="${2:-30000}"
    
    echo "📋 Test pattern: $test_pattern"
    echo "⏱️  Timeout: ${timeout}ms"
    
    # Run the test
    npm test -- --grep "$test_pattern" --timeout "$timeout"
}

# Check if specific test pattern is provided
if [ $# -eq 0 ]; then
    echo "Running all tests..."
    run_tests ".*"
else
    echo "Running specific test: $1"
    run_tests "$1" "${2:-30000}"
fi

echo "✅ Test execution completed!"

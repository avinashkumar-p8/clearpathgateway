#!/bin/bash

echo "🧹 Cleaning up Docker services..."

# Stop all running containers
echo "Stopping containers..."
docker-compose down

# Remove any dangling containers
echo "Removing dangling containers..."
docker container prune -f

# Remove any dangling networks
echo "Removing dangling networks..."
docker network prune -f

# Remove any dangling volumes (optional - uncomment if needed)
# echo "Removing dangling volumes..."
# docker volume prune -f

echo "✅ Docker cleanup completed!"
echo "📊 Final status:"
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

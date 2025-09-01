#!/bin/bash

echo "🧹 Complete Docker Cleanup for ClearPath Gateway"
echo "=================================================="

# Change to project root
cd "$(dirname "$0")"

echo "📁 Project directory: $(pwd)"

# Stop all Docker Compose services
echo "🛑 Stopping all Docker Compose services..."
docker-compose down

# Stop any other running containers
echo "🛑 Stopping any other running containers..."
docker stop $(docker ps -q) 2>/dev/null || echo "No running containers found"

# Remove all containers
echo "🗑️  Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null || echo "No containers to remove"

# Remove all networks
echo "🗑️  Removing all networks..."
docker network prune -f

# Remove all volumes (optional - uncomment if you want to remove data)
# echo "🗑️  Removing all volumes..."
# docker volume prune -f

# Remove all images (optional - uncomment if you want to remove images)
# echo "🗑️  Removing all images..."
# docker image prune -a -f

echo ""
echo "✅ Complete cleanup finished!"
echo ""
echo "📊 Final Docker status:"
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "🌐 Networks:"
docker network ls --format "table {{.Name}}\t{{.Driver}}\t{{.Scope}}"
echo ""
echo "💾 Volumes:"
docker volume ls --format "table {{.Name}}\t{{.Driver}}"

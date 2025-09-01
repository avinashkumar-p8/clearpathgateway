# 🐳 Docker Cleanup Scripts for ClearPath Gateway

This document explains how to use the automated Docker cleanup scripts to manage Docker services during development and testing.

## 📁 Available Scripts

### 1. **Root Level Cleanup** (`cleanup-all.sh`)
**Location**: Project root directory  
**Purpose**: Complete cleanup of all Docker resources  
**Usage**: `./cleanup-all.sh`

**What it does**:
- Stops all Docker Compose services
- Stops any running containers
- Removes all containers
- Removes all networks
- Shows final Docker status

### 2. **Playwright Level Cleanup** (`playwright/cleanup-docker.sh`)
**Location**: `playwright/` directory  
**Purpose**: Cleanup after Playwright tests  
**Usage**: `./cleanup-docker.sh`

**What it does**:
- Stops containers from `docker-compose.yml`
- Removes dangling containers and networks
- Shows final status

## 🚀 **Automatic Cleanup**

### **After Playwright Tests**
The `posttest` script automatically runs cleanup after tests:
```bash
npm test                    # Run tests
# Automatically runs: npm run docker:down && ./cleanup-docker.sh
```

### **Manual Cleanup Commands**
```bash
# From project root
./cleanup-all.sh           # Complete cleanup
npm run cleanup:all        # Alternative via npm

# From playwright directory
./cleanup-docker.sh        # Playwright-specific cleanup
npm run cleanup            # Via npm
```

## 🔧 **NPM Scripts Available**

```json
{
  "cleanup": "./cleanup-docker.sh",
  "cleanup:all": "docker-compose down && ./cleanup-docker.sh",
  "posttest": "npm run docker:down && ./cleanup-docker.sh"
}
```

## 📋 **Cleanup Process**

1. **Stop Services**: `docker-compose down`
2. **Stop Containers**: `docker stop $(docker ps -q)`
3. **Remove Containers**: `docker rm $(docker ps -aq)`
4. **Clean Networks**: `docker network prune -f`
5. **Show Status**: Display final Docker state

## ⚠️ **Important Notes**

- **Data Loss**: Cleanup removes all containers and networks
- **Volumes**: Data volumes are preserved by default
- **Images**: Docker images are preserved by default
- **Automatic**: Runs automatically after Playwright tests

## 🎯 **Best Practices**

1. **Always run cleanup** after testing is complete
2. **Use `cleanup-all.sh`** for complete system reset
3. **Check status** after cleanup to ensure clean state
4. **Preserve important data** by backing up volumes if needed

## 🚨 **Troubleshooting**

If cleanup fails:
```bash
# Force stop all containers
docker kill $(docker ps -q)

# Force remove all containers
docker rm -f $(docker ps -aq)

# Restart Docker Desktop if needed
```

## 📊 **Example Output**

```
🧹 Complete Docker Cleanup for ClearPath Gateway
==================================================
📁 Project directory: /Users/.../clearpathgateway
🛑 Stopping all Docker Compose services...
🛑 Stopping any other running containers...
🗑️  Removing all containers...
🗑️  Removing all networks...
✅ Complete cleanup finished!

📊 Final Docker status:
NAMES     STATUS    PORTS

🌐 Networks:
NAME      DRIVER    SCOPE
bridge    bridge    local
host      host      local
none      null      local
```

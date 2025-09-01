#!/usr/bin/env node

/**
 * Playwright Test Runner with Docker Lifecycle Management
 *
 * This script provides robust Docker Compose stack management for Playwright tests.
 * Features:
 * - Cross-platform Docker CLI detection (docker-compose vs docker compose plugin)
 * - Absolute path resolution for compose files
 * - Guaranteed cleanup on success/failure/signals
 * - Proper working directory management
 * - Comprehensive error handling
 * - Configurable Schema Registry readiness check with timeout
 *
 * Environment Variables:
 * - SCHEMA_REGISTRY_URL: Schema Registry base URL (default: http://localhost:8081)
 * - KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers (default: localhost:9092)
 */

const { spawn, exec } = require('child_process');
const path = require('path');
const fs = require('fs');

// ANSI color codes for better output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m'
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

function logError(message) {
  console.error(`${colors.red}${message}${colors.reset}`);
}

function logSuccess(message) {
  console.log(`${colors.green}${message}${colors.reset}`);
}

function logInfo(message) {
  console.log(`${colors.blue}${message}${colors.reset}`);
}

function logWarning(message) {
  console.log(`${colors.yellow}${message}${colors.reset}`);
}

// Detect which Docker CLI to use
function detectDockerCLI() {
  // Try docker-compose first (legacy standalone)
  try {
    require('child_process').execSync('docker-compose --version', { stdio: 'ignore' });
    logInfo('🔍 Using docker-compose (legacy standalone)');
    return ['docker-compose'];
  } catch (error) {
    // Fall back to docker compose plugin
    try {
      require('child_process').execSync('docker compose version', { stdio: 'ignore' });
      logInfo('🔍 Using docker compose plugin');
      return ['docker', 'compose'];
    } catch (error) {
      throw new Error('Neither docker-compose nor docker compose plugin found. Please install Docker and ensure docker-compose is available.');
    }
  }
}

// Resolve absolute paths and set working directory
function getDockerComposeConfig() {
  const playwrightDir = __dirname;
  const composeFile = path.resolve(playwrightDir, 'docker-compose-test.yml');

  // Verify the compose file exists
  if (!fs.existsSync(composeFile)) {
    throw new Error(`Docker Compose file not found: ${composeFile}`);
  }

  return {
    composeFile,
    workingDir: playwrightDir,
    dockerCLI: detectDockerCLI()
  };
}

// Validate and normalize Schema Registry URL
function getSchemaRegistryConfig() {
  const baseUrl = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';

  // Ensure URL has protocol
  if (!baseUrl.startsWith('http://') && !baseUrl.startsWith('https://')) {
    throw new Error(`Invalid Schema Registry URL: ${baseUrl}. Must start with http:// or https://`);
  }

  // Remove trailing slash if present
  const normalizedUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;

  return {
    baseUrl: normalizedUrl,
    checkEndpoint: `${normalizedUrl}/subjects`
  };
}

// Ensure Docker services are torn down
async function ensureDockerDown() {
  return new Promise((resolve) => {
    try {
      const config = getDockerComposeConfig();
      logInfo('🧹 Cleaning up Docker services...');

      const dockerDown = spawn(config.dockerCLI[0], [...config.dockerCLI.slice(1), '-f', config.composeFile, 'down'], {
        stdio: 'inherit',
        shell: true,
        cwd: config.workingDir
      });

      dockerDown.on('close', (code) => {
        if (code === 0) {
          logSuccess('✅ Docker services cleaned up successfully');
        } else {
          logWarning(`⚠️  Docker cleanup exited with code ${code}`);
        }
        resolve();
      });

      dockerDown.on('error', (err) => {
        logError(`❌ Failed to run docker compose down: ${err.message}`);
        resolve();
      });
    } catch (error) {
      logError(`❌ Failed to get Docker Compose configuration: ${error.message}`);
      resolve();
    }
  });
}

// Start Docker services
async function startDockerServices() {
  return new Promise((resolve, reject) => {
    try {
      const config = getDockerComposeConfig();
      logInfo('🚀 Starting Docker services...');

      const dockerUp = spawn(config.dockerCLI[0], [...config.dockerCLI.slice(1), '-f', config.composeFile, 'up', '-d'], {
        stdio: 'inherit',
        shell: true,
        cwd: config.workingDir
      });

      dockerUp.on('close', (code) => {
        if (code === 0) {
          logSuccess('✅ Docker services started successfully');
          resolve();
        } else {
          reject(new Error(`Docker services failed to start (exit code: ${code})`));
        }
      });

      dockerUp.on('error', (err) => {
        reject(new Error(`Failed to start Docker services: ${err.message}`));
      });
    } catch (error) {
      reject(new Error(`Failed to get Docker Compose configuration: ${error.message}`));
    }
  });
}

// Wait for services to be ready
async function waitForServices() {
  return new Promise((resolve, reject) => {
    try {
      const config = getSchemaRegistryConfig();
      logInfo('⏳ Waiting for services to be ready...');

      const maxWaitMs = 90_000; // 90 seconds maximum wait
      const retryIntervalMs = 2000; // 2 seconds between retries
      const initialDelayMs = 5000; // 5 seconds initial delay

      logInfo(`🔍 Checking Schema Registry at: ${config.checkEndpoint}`);

      // Set up timeout to reject the promise after max wait
      const timeoutId = setTimeout(() => {
        reject(new Error(`Schema Registry not ready after ${maxWaitMs / 1000} seconds. Check if the service is running at ${config.baseUrl}`));
      }, maxWaitMs);

      // Wait for Schema Registry to be ready
      const checkSchemaRegistry = () => {
        exec(`curl --fail --silent --show-error "${config.checkEndpoint}"`, (error) => {
          if (error) {
            logInfo('⏳ Schema Registry not ready yet, waiting...');
            setTimeout(checkSchemaRegistry, retryIntervalMs);
          } else {
            clearTimeout(timeoutId); // Clear the timeout on success
            logSuccess('✅ Schema Registry is ready');
            resolve();
          }
        });
      };

      // Start checking after initial delay
      setTimeout(checkSchemaRegistry, initialDelayMs);
    } catch (error) {
      reject(new Error(`Failed to configure Schema Registry check: ${error.message}`));
    }
  });
}

// Run Playwright tests
async function runTests() {
  return new Promise((resolve, reject) => {
    try {
      const config = getDockerComposeConfig();
      logInfo('🧪 Running Playwright tests...');

      const testProcess = spawn('npx', ['playwright', 'test'], {
        stdio: 'inherit',
        shell: true,
        cwd: config.workingDir
      });

      testProcess.on('close', (code) => {
        if (code === 0) {
          logSuccess('✅ Tests completed successfully');
          resolve();
        } else {
          reject(new Error(`Tests failed with exit code: ${code}`));
        }
      });

      testProcess.on('error', (err) => {
        reject(new Error(`Failed to run tests: ${err.message}`));
      });
    } catch (error) {
      reject(new Error(`Failed to get configuration: ${error.message}`));
    }
  });
}

// Main execution function
async function main() {
  let dockerStarted = false;

  try {
    // Set up cleanup handlers
    const cleanup = async () => {
      if (dockerStarted) {
        await ensureDockerDown();
      }
    };

    // Handle various exit scenarios
    process.on('SIGINT', async () => {
      logWarning('\n⚠️  Received SIGINT, cleaning up...');
      await cleanup();
      process.exit(1);
    });

    process.on('SIGTERM', async () => {
      logWarning('\n⚠️  Received SIGTERM, cleaning up...');
      await cleanup();
      process.exit(1);
    });

    process.on('uncaughtException', async (err) => {
      logError(`\n❌ Uncaught exception: ${err.message}`);
      await cleanup();
      process.exit(1);
    });

    process.on('unhandledRejection', async (reason, promise) => {
      logError(`\n❌ Unhandled rejection at: ${promise}, reason: ${reason}`);
      await cleanup();
      process.exit(1);
    });

    // Start Docker services
    await startDockerServices();
    dockerStarted = true;

    // Wait for services to be ready
    await waitForServices();

    // Run tests
    await runTests();

    logSuccess('\n🎉 All tests completed successfully!');

  } catch (error) {
    logError(`\n❌ Error: ${error.message}`);
    process.exitCode = 1;
  } finally {
    // Always ensure cleanup
    if (dockerStarted) {
      await ensureDockerDown();
    }
  }
}

// Run the main function
if (require.main === module) {
  main().catch((error) => {
    logError(`\n💥 Fatal error: ${error.message}`);
    process.exit(1);
  });
}

module.exports = { main, startDockerServices, runTests, ensureDockerDown };

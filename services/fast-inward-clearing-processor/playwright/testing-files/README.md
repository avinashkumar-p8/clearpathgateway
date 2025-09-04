# Testing Files Directory

This directory contains all testing-related files that were moved from the root project directory to keep the main project clean and focused on business logic.

## Directory Structure

```
testing-files/
├── README.md                           # This file
├── config/                            # Test configuration files
│   ├── docker-compose.yml             # Docker compose for testing
│   ├── docker-dev.sh                  # Docker development script
│   ├── docker.env                     # Docker environment variables
│   └── env.local                      # Local environment configuration
├── data/                              # Test data files
│   ├── dummy-credentials.json         # Dummy Spanner credentials
│   └── schema-payload.json            # Schema payload for testing
├── docs/                              # Test documentation
│   ├── COMPLETE_FLOW_DOCUMENTATION.md # Complete flow documentation
│   └── SCHEMA_EVOLUTION_GUIDE.md      # Schema evolution guide
├── scripts/                           # Test scripts
│   ├── clear-kafka-topics.sh          # Clear Kafka topics
│   ├── create-tables-manual.sh        # Manual table creation
│   ├── init-spanner-db.sql            # Spanner database initialization
│   ├── init-spanner-docker.sh         # Docker Spanner initialization
│   ├── init-spanner-rest.sh           # REST API Spanner initialization
│   ├── init-spanner.sh                # General Spanner initialization
│   ├── register-schema.sh             # Schema registration script
│   ├── send-avro-schema-event.sh      # Send Avro schema event
│   ├── send-schema-event.sh           # Send schema event
│   └── trigger-schema-registration.sh # Trigger schema registration
├── start-local-dev.sh                 # Start local development
├── start-local.sh                     # Start local environment
├── stop-local-dev.sh                  # Stop local development
├── stop-local.sh                      # Stop local environment
├── run-tests-automated.sh             # Automated test runner
├── cleanup-temp-changes.sh            # Cleanup temporary changes
├── README-LOCAL-DEV.md                # Local development README
└── README-LOCAL.md                    # Local environment README
```

## Usage

These files are organized for easy access during testing and development:

- **Scripts**: Run test setup, cleanup, and utility scripts
- **Config**: Docker and environment configuration for testing
- **Data**: Test data and dummy credentials
- **Docs**: Testing documentation and guides

## Note

These files are **NOT part of the production build** and are only used for:
- Local development
- Testing
- Debugging
- Documentation

The main project directory now contains only the essential business logic and production configuration files.

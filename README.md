# ClearPath Gateway - Singapore Fast Payment System

## Overview

The APEAFAST-SG Clear Path Gateway is a comprehensive cloud-native payment processing platform that replaces GPP G3 for Singapore's faster payment system. This mono repository contains all the microservices that make up the payment gateway architecture.

## Architecture

The solution follows a hybrid architecture model with microservices running in cloud regions, providing end-to-end payment processing capabilities with integrated liquidity management, real-time monitoring, and operational excellence.

### Core Services

| Service | Purpose | Description |
|---------|---------|-------------|
| **fast-router-service** | Message Routing | Entry point for all messages from CPG/G3 Host, handles routing and initial validation |
| **fast-inward-clearing-processor** | Inbound Payments | Processes CTI (Credit Transfer Inward) and DDI (Direct Debit Inward) with 4.5s SLA |
| **fast-sender-service** | Message Transmission | Handles outbound message transmission to G3 Host via CPG |
| **fast-outward-clearing-processor** | Outbound Payments | Processes CTO (Credit Transfer Outward) with liquidity authorization |
| **fast-liquidity-service** | Liquidity Management | Real-time liquidity tracking, net debit cap monitoring, authorization engine |
| **fast-availability-service** | Bank Status | Manages participant bank availability status and service interruption notifications |

## Repository Structure

```
clearpathgateway/
├── services/                          # Core microservices
│   ├── fast-router-service/           # Message routing and validation
│   ├── fast-inward-clearing-processor/ # Inbound payment processing
│   ├── fast-sender-service/           # Outbound message transmission
│   ├── fast-outward-clearing-processor/ # Outbound payment processing
│   ├── fast-liquidity-service/        # Liquidity management
│   └── fast-availability-service/     # Bank availability management
├── shared/                            # Shared libraries and utilities
├── infrastructure/                    # Infrastructure as Code (Terraform, K8s)
├── monitoring/                        # Monitoring and observability configs
├── config/                           # Configuration files
├── deployments/                      # Deployment manifests
├── scripts/                          # Build and deployment scripts
├── tests/                           # Integration and E2E tests
└── docs/                            # Documentation

```

## Key Features

- **High Performance**: 20+ TPS capacity with horizontal scaling
- **SLA Compliance**: 4.5-second processing time for inward payments
- **High Availability**: 99.95% uptime target
- **Real-time Processing**: Event-driven architecture with Kafka
- **Regulatory Compliance**: MAS compliance for Singapore
- **Multi-protocol Support**: PACS.008, PACS.003, PACS.002, CAMT.056, etc.

## Technology Stack

- **Platform**: Google Cloud Platform (GCP)
- **Orchestration**: Kubernetes (GKE)
- **Messaging**: Apache Kafka
- **Databases**: Cloud Spanner, BigQuery, Redis, Firestore
- **Languages**: Java/Spring Boot, Go (planned)
- **Monitoring**: Prometheus, Grafana, Jaeger

## Quick Start

### Prerequisites
- Docker and Docker Compose
- kubectl and gcloud CLI
- Java 11+ or Go 1.19+

### Local Development
```bash
# Clone the repository
git clone git@github.com:hari-p8-io/clearpathgateway.git
cd clearpathgateway

# Start local development environment
./scripts/dev-setup.sh

# Run all services locally
docker-compose up -d
```

### Deployment
```bash
# Deploy to GCP
./scripts/deploy.sh staging
```

## Payment Flows

### Inward Credit Transfer (CTI)
G3 Host → CPG → fast-router-service → fast-inward-clearing-processor → Response

### Outward Credit Transfer (CTO)
PSP Global → fast-outward-clearing-processor → fast-liquidity-service → fast-sender-service → G3 Host

### Direct Debit Inward (DDI)
G3 Host → CPG → fast-router-service → fast-inward-clearing-processor → fast-liquidity-service → Response

## Contributing

Please read our [contribution guidelines](docs/CONTRIBUTING.md) before submitting pull requests.

## License

Proprietary - ANZ Bank

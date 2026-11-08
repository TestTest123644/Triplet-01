# Infrastructure as Code (IaC) — Guardian Layer Provisioning Plan

## 1. Overview
This document specifies the Infrastructure as Code (IaC) architecture and provisioning plan for the Guardian Layer using Terraform / OpenTofu. The infrastructure manages containerized and cloud resources across ingestion brokers, secret engines, persistent state stores, and analytical logging clusters.

## 2. Managed Infrastructure Modules

### 2.1 Storage & Broker Cluster
* **Apache Kafka (KRaft mode):** 3 broker nodes deployed across isolated availability zones with pre-provisioned topics:
  - `telemetry-ingest` (alias `sensors.raw`): 12 partitions, replication factor 3.
  - `telemetry-validated` (alias `sensors.validated`): 12 partitions, replication factor 3.
  - `telemetry-dlq` (alias `sensors.dlq`): 6 partitions, 30-day retention for forensics.
  - `devices.metadata`: 3 partitions, compacted cleanup policy.

### 2.2 Security Perimeter
* **HashiCorp Vault Cluster:** 3-node HA deployment utilizing Raft integrated storage with auto-unseal via cloud KMS.
* **AppRole Provisioning:** Automated registration of `guardian-app`, `twin-app`, `brain-app`, and `rotator` roles with least-privilege path policies.
* **Network Access Control:** Docker network segmentation isolating external ingest ports from internal database and Vault listeners.

### 2.3 State & Observability Databases
* **PostgreSQL 16:** Managed relational instance with automated daily snapshots and connection pooling for verdict persistence.
* **Elasticsearch 8.15:** 3-node analytical cluster with index lifecycle management (ILM) for 90-day forensic log retention.

## 3. Terraform State & Secret Management
* **Remote Backend:** S3-compatible bucket / GCS backend with state locking via DynamoDB / Cloud Storage mutex.
* **Secret Injection:** Zero hard-coded credentials; database and broker passwords dynamically pulled from Vault via the Terraform Vault Provider.

## 4. Deployment Workflow
```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```


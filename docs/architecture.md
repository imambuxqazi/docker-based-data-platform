# Data Platform Architecture

## Overview

This project implements a local end-to-end data platform using Docker Compose. It simulates two production-style ingestion patterns:

1. **Polling-based CDC** from PostgreSQL using `updated_at` watermarks
2. **Event streaming** from Redpanda using Kafka-compatible structured streaming

The platform processes data through three analytical layers:

- **Bronze**: raw landed data
- **Silver**: validated, standardized, deduplicated Delta tables
- **Gold**: business-facing marts and KPI tables

It also includes:

- data generation
- data quality validation
- alert persistence
- Airflow orchestration
- analytics queries

---
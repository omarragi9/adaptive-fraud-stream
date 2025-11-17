# Adaptive Fraud-Scoring Stream

Local end-to-end demo of a streaming fraud scoring pipeline using NiFi, Kafka, Spark (ephemeral), and Airflow.

Project skeleton for the Adaptive Fraud-Scoring Stream (NiFi → Kafka → Spark).
- Steps: ingestion (NiFi) → Kafka topics → Spark streaming scorer → label queue → retrain.
- Requirements: Java (installed), Python >= 3.8 , git, Kafka in WSL, NiFi local, Spark local , Airflow in Docker.


## What it does
- NiFi generates synthetic transactions -> publishes to Kafka topic `transactions.raw`
- A PySpark Structured Streaming job reads `transactions.raw`, computes a simple score, writes scored transactions to Parquet, and emits alerts (`score >= 1.0`) to topic `transactions.alerts`
- NiFi `alerts_consumer` consumes `transactions.alerts` and appends labels to `label_queue/labels.csv`
- Airflow DAG `trigger_nifi_process_group` orchestrates starting the NiFi process group, waiting for a configurable duration, then stopping it (used for ephemeral runs).

## Quick start (local)
1. Install prerequisites: Java 17, Spark 3.5.x, Kafka, NiFi 2.6.0, Docker (for Airflow).
2. Configure `.env` under `infra/airflow` (see `.env.example`).
3. Start NiFi and Kafka locally.
4. Run Airflow: `cd infra/airflow && docker compose up -d`
5. Trigger DAG `trigger_nifi_process_group` from Airflow UI (http://localhost:8085).
6. Check `data/transactions_scored` and `label_queue/labels.csv`.

## Developer notes
- Spark app supports ephemeral runs: `--run-seconds` and `--run-id` to isolate checkpoints.
- Checkpoint paths are at `data/checkpoints/.../<run_id>` when `--run-id` is provided.
- NiFi credentials are stored in `infra/airflow/.env` for local dev only (do not commit).

## License
GNU


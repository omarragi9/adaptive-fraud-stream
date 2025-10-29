# Adaptive Fraud-Scoring Stream

Project skeleton for the Adaptive Fraud-Scoring Stream (NiFi → Kafka → Spark).
- Steps: ingestion (NiFi) → Kafka topics → Spark streaming scorer → label queue → retrain.
- Requirements: Java (installed), Python >= 3.8 (you currently have Python 3.7), git, Kafka in WSL, NiFi local, Spark local.

See docs/ for next steps.

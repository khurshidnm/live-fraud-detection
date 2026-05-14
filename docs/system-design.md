## System Design Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                           │
│  Synthetic Fraud Dataset (CSV) / Real Transaction Streams                      │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│               INGESTION LAYER (Kafka)                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Topic: raw-transactions (3 partitions)                                │    │
│  │ • Transaction ID, User ID, Amount, Merchant, Timestamp, Location     │    │
│  │ • Replication factor: 1 (dev), 3 (prod)                              │    │
│  │ • Retention: 7 days                                                   │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│           STREAM PROCESSING LAYER (Spark Structured Streaming)                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ BRONZE LAYER (Raw Ingestion)                                           │   │
│  │ • Schema enforcement                                                   │   │
│  │ • Append-only Delta table: /data/delta/bronze                         │   │
│  │ • No transformations (raw preservation)                               │   │
│  │ • Checkpoint: /data/checkpoints/bronze                                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ SILVER LAYER (Feature Engineering + Scoring)                           │   │
│  │ • 10 derived features (log_amount, hour, bucket, risk_score, etc.)    │   │
│  │ • 5 fraud signals (high_amount, velocity, off_hours, geo, merchant)   │   │
│  │ • Rule-based scoring (0-1 range)                                       │   │
│  │ • Delta table: /data/delta/silver                                      │   │
│  │ • ML score integration (if model loaded)                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ GOLD LAYER (Fraud Flagging + Outputs)                                  │   │
│  │ • Final fraud_score: max(rule_score, ml_score)                         │   │
│  │ • Flag decision: is_flagged = (fraud_score ≥ 0.35)                    │   │
│  │ • Delta table: /data/delta/gold                                        │   │
│  │ • Outputs:                                                              │   │
│  │   ├─ Kafka: flagged-transactions topic                                │   │
│  │   ├─ PostgreSQL: fraud_metrics table                                  │   │
│  │   └─ Dashboards: Grafana (real-time)                                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│  • Micro-batch trigger: 10 seconds                                            │
│  • Checkpointing: /data/checkpoints/main                                      │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
    (Delta Lake)            (Kafka Topic)          (PostgreSQL)
   Gold Layer Data       flagged-transactions      fraud_metrics
   (Historical)          (Streaming)               (Analytics)
        │                         │                         │
        └─────────────────────────┼─────────────────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│               ML LAYER (XGBoost + MLflow)                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Daily Retraining Pipeline                                             │    │
│  │ • Input: Silver layer data from yesterday                             │    │
│  │ • Model: XGBoost classifier                                           │    │
│  │ • Metrics: ROC-AUC, PR-AUC, Precision, Recall, F1                   │    │
│  │ • Tracking: MLflow experiments & runs                                 │    │
│  │ • Output: fraud_model.pkl (shared volume)                             │    │
│  │ • Versioning: Staging → Production → Archived                         │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Model Evaluation & Promotion                                          │    │
│  │ • Compare new model vs production                                     │    │
│  │ • Promotion criteria: PR-AUC improvement ≥ 2%                        │    │
│  │ • Automatic or manual approval                                        │    │
│  │ • Audit trail in model_registry table                                 │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
            (Shared Volume)              (PostgreSQL)
         fraud_model.pkl              model_registry
            (Streaming uses)           (Audit trail)
                    │                           │
                    └─────────────┬─────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│          ORCHESTRATION LAYER (Apache Airflow 2.8)                               │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ fraud_detection_daily_dag (00:00 UTC)                                 │    │
│  │ • validate_silver → decide_retrain → retrain_model                    │    │
│  │ • → evaluate_model → promote_gold → send_notification                 │    │
│  │ • Timeout: 2 hours                                                    │    │
│  │ • Retries: 2 on failure                                               │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ data_quality_monitoring_dag (hourly)                                  │    │
│  │ • bronze_row_count_check, schema_check, null_rate_check              │    │
│  │ • fraud_rate_check, consistency_check                                 │    │
│  │ • Results → PostgreSQL dq_checks table                                │    │
│  │ • Timeout: 10 minutes                                                 │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│             ANALYTICS LAYER (Grafana + PostgreSQL)                              │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Grafana Dashboards                                                    │    │
│  │ • KPI Cards: Fraud rate, Detection rate, Model accuracy              │    │
│  │ • Time Series: Flagged transactions, Trends                          │    │
│  │ • Heatmaps: Fraud by hour/merchant                                   │    │
│  │ • Data Quality: Health checks, anomalies                             │    │
│  │ • Model Performance: Feature importance, metrics history             │    │
│  │ • Alerting: Rules for anomalies                                      │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ PostgreSQL Analytics Database                                         │    │
│  │ • fraud_metrics: Real-time scoring results                            │    │
│  │ • dq_checks: Data quality metrics                                     │    │
│  │ • model_registry: ML model versions and performance                   │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```
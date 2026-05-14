## Data Architecture

### Data Layers (Medallion Pattern)

#### **Layer 1: Bronze (Raw Data)**
```
Purpose: Preserve raw data as-is for replay and audit
Location: /data/delta/bronze
Characteristics:
  - Append-only (immutable)
  - Schema enforced (no corruption)
  - No transformations
  - High volume retention (7+ days)
  
Schema:
  {
    "_record_id": "uuid",           // Unique record identifier
    "_ingested_at": "2026-05-14...", // When ingested
    "Transaction_ID": "TXN_001",
    "User_ID": "USER_42",
    "Transaction_Amount": 5000.00,
    "Transaction_Type": "Online",
    "Timestamp": "2026-05-14...",
    "Merchant_Category": "Gambling",
    "Fraud_Label": 1,
    // ... 16 more features
  }

Write Pattern: StreamWriter (foreachBatch)
Read Pattern: Ad-hoc replay, backfill, debugging
Retention: 7 days (configurable)
Replication: 1 (dev), 3 (prod)
```

#### **Layer 2: Silver (Refined Data)**
```
Purpose: Feature engineering, ML preparation, and scoring
Location: /data/delta/silver
Characteristics:
  - All Bronze data + derived features
  - Schema enforced
  - Stateful aggregations (daily counts, velocity)
  - Ready for ML training
  
Additional Features (10 derived):
  - log_amount: Normalized transaction amount
  - event_hour: Hour of day (0-23)
  - amount_bucket: Categorical [low, medium, high, very_high]
  - merchant_risk_score: Lookup table (0-1)
  - day_of_week: Day (0-6)
  - is_weekend: Binary flag
  - transaction_count_today: Daily count per user
  - failed_transactions_7d: Count of failed txns in 7 days
  - hours_since_last_txn: Time since previous transaction
  - fraud_history: Whether user had fraud before
  
Fraud Signals (5 flags):
  - flag_high_amount: amount > $3,000
  - flag_velocity: daily_count ≥ 6 OR failed_7d ≥ 3
  - flag_off_hours: hour in [2, 3, 4]
  - flag_geo_anomaly: distance > 75km from usual location
  - flag_risky_merchant: merchant in [crypto, gambling, wire_transfer]
  
Rule Score Computation:
  rule_score = (
    0.25 * flag_high_amount +
    0.25 * flag_velocity +
    0.15 * flag_off_hours +
    0.20 * flag_risky_merchant +
    0.15 * flag_geo_anomaly
  )
  → Score range: [0, 1]

Write Pattern: StreamWriter (foreachBatch, stateful aggregations)
Read Pattern: ML training, dashboards, analysis
Update Frequency: 10-second micro-batches
Stateful Keys: (user_id, date) for daily aggregations
```

#### **Layer 3: Gold (Business Data)**
```
Purpose: Flagged transactions for investigation and action
Location: /data/delta/gold
Characteristics:
  - Filtered to is_flagged = 1 only
  - Business-ready format
  - Fast queries for dashboards
  - Event stream for action
  
Schema:
  {
    "transaction_id": "TXN_001",
    "user_id": "USER_42",
    "timestamp": "2026-05-14...",
    "amount": 5000.00,
    "merchant": "Crypto Exchange",
    "fraud_score": 0.72,
    "rule_score": 0.65,
    "ml_score": 0.72,
    "fraud_flags": ["flag_high_amount", "flag_risky_merchant"],
    "confidence": "high",
    "action_recommended": "BLOCK"
  }

Write Pattern: StreamWriter (filtered)
Read Pattern: Investigation, case management, dashboards
Output Destinations:
  - Delta Lake (historical archive)
  - Kafka topic "flagged-transactions" (real-time stream)
  - PostgreSQL "fraud_metrics" table (analytics)
  - Grafana dashboards (live visualization)
```

### Data State Management

```
Streaming State (Spark Stateful Aggregations):
┌──────────────────────────────────────┐
│ State Store (/data/checkpoints/state)│
├──────────────────────────────────────┤
│ daily_transaction_count[user][date]  │ Updates every micro-batch
│ failed_transactions[user][date]      │ Aggregates over 7 days
│ last_transaction_time[user]          │ Latest timestamp
│ user_fraud_history[user]             │ Ever had fraud?
└──────────────────────────────────────┘

Checkpoint Locations:
├─ /data/checkpoints/bronze     (Bronze writer)
├─ /data/checkpoints/silver     (Silver writer with state)
├─ /data/checkpoints/main       (Gold writer)
└─ /data/checkpoints/state      (State store for aggregations)

Purpose:
  - Enables recovery from failures
  - Exactly-once processing semantics
  - No data loss or duplicates
  - Can replay from checkpoints
```

---
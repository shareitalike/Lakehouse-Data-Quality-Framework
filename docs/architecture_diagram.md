# Architecture Diagram — Data Quality + Observability Framework

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE DATA QUALITY + OBSERVABILITY FRAMEWORK             │
│                         Medallion Architecture (Bronze → Silver → Gold)         │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────┐
                    │    RULE CONFIG ENGINE     │
                    │  (Python Dataclasses)     │
                    │                          │
                    │  • NotNullRule            │
                    │  • UniqueKeyRule          │
                    │  • AcceptedValuesRule     │
                    │  • PositiveNumericRule    │
                    │  • FreshnessRule          │
                    │  • DuplicateRule          │
                    │  • SchemaDriftRule        │
                    │  • RefIntegrityRule       │
                    │  • RowCountAnomalyRule    │
                    │  • DistributionRule       │
                    └────────────┬─────────────┘
                                 │ feeds
                                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│   RAW EVENTS ──► ┌─────────┐    ┌─────────┐    ┌─────────┐                   │
│   (E-Commerce)   │ BRONZE  │───►│ SILVER  │───►│  GOLD   │                   │
│                  │  Layer  │    │  Layer  │    │  Layer  │                   │
│                  └────┬────┘    └────┬────┘    └────┬────┘                   │
│                       │             │              │                         │
│                  ┌────▼────┐   ┌────▼────┐   ┌────▼────┐                    │
│                  │VALIDATE │   │VALIDATE │   │VALIDATE │                    │
│                  │CHECKPOINT│  │CHECKPOINT│  │CHECKPOINT│                   │
│                  └──┬───┬──┘   └──┬───┬──┘   └──┬───┬──┘                    │
│                     │   │         │   │          │   │                        │
│              ┌──────┘   └──┐  ┌──┘   └──┐   ┌──┘   └──┐                    │
│              ▼             ▼  ▼          ▼   ▼          ▼                    │
│         ┌────────┐   ┌─────────┐   ┌────────┐   ┌─────────┐                │
│         │  PASS  │   │QUARANTINE│  │  PASS  │   │QUARANTINE│               │
│         │(next   │   │ (Delta  │   │(next   │   │ (Delta  │                │
│         │ layer) │   │  table) │   │ layer) │   │  table) │                │
│         └────────┘   └─────────┘   └────────┘   └─────────┘                │
│                                                                               │
│   ┌───────────────────────────────────────────────────────────────────┐       │
│   │              OBSERVABILITY METRICS LAYER                          │       │
│   │                                                                   │       │
│   │  Captures: null_rate │ dup_rate │ freshness_lag │ schema_drift   │       │
│   │           pass_rate │ distribution_drift │ row_count_delta       │       │
│   │                                                                   │       │
│   │  ┌─────────────────────────────────────────────────────────┐     │       │
│   │  │  OBSERVABILITY DELTA TABLE (Append-Only)                │     │       │
│   │  │  run_id | dataset | layer | rule | metric | value | ts  │     │       │
│   │  └─────────────────────────────────────────────────────────┘     │       │
│   │                         │                                         │       │
│   │                         ▼                                         │       │
│   │              ┌────────────────────┐                               │       │
│   │              │  SQL OBSERVABILITY │                               │       │
│   │              │  QUERIES           │                               │       │
│   │              │  (dbt-style)       │                               │       │
│   │              └────────────────────┘                               │       │
│   └───────────────────────────────────────────────────────────────────┘       │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────────┐
         │         FEEDBACK LOOP                     │
         │  Observability → Trend Analysis           │
         │  → Rule Tuning → Config Update            │
         │  → Next Pipeline Run                      │
         └──────────────────────────────────────────┘
```

## Data Flow Detail

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                                     │
│                                                                         │
│  Input: Raw e-commerce order events (JSON/CSV)                          │
│                                                                         │
│  Injected Issues:                                                       │
│  ├── null customer_id (~5%)                                             │
│  ├── duplicate order_id (~3%)                                           │
│  ├── malformed timestamps (~2%)                                         │
│  ├── unexpected order_status enums (~2%)                                │
│  ├── schema drift (new column appears)                                  │
│  ├── missing column scenario                                            │
│  ├── negative quantity (~1%)                                            │
│  ├── invalid product_id FK (~3%)                                        │
│  └── currency mismatch (~2%)                                            │
│                                                                         │
│  Validation: schema_drift_detection, not_null_check (critical fields)   │
│  Output: Delta table (schema enforcement mode)                          │
│  Quarantine: records failing critical rules                             │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                                     │
│                                                                         │
│  Input: Bronze Delta table (validated subset)                           │
│                                                                         │
│  Transformations:                                                       │
│  ├── Deduplicate by order_id (keep latest)                              │
│  ├── Normalize timestamps to UTC                                        │
│  ├── Split into: orders, customers, products                            │
│  ├── Apply referential integrity checks                                 │
│  └── Handle late-arriving records (MERGE)                               │
│                                                                         │
│  Issues Present:                                                        │
│  ├── Referential integrity failures                                     │
│  ├── Late arriving records                                              │
│  └── Schema evolution scenario                                          │
│                                                                         │
│  Validation: all 10 rules applied                                       │
│  Output: 3 Delta tables (schema evolution mode)                         │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                      │
│                                                                         │
│  Input: Silver Delta tables (orders, customers, products)               │
│                                                                         │
│  Aggregations:                                                          │
│  ├── daily_revenue (date, total_revenue, order_count)                   │
│  ├── product_performance (product_id, total_sold, revenue, avg_price)   │
│  └── customer_lifetime_value (customer_id, total_orders, total_spend)   │
│                                                                         │
│  Injected Anomalies:                                                    │
│  ├── Row count anomaly (sudden spike/drop)                              │
│  ├── Freshness SLA breach (stale data)                                  │
│  └── Aggregation skew (single product dominates)                        │
│                                                                         │
│  Validation: row_count_anomaly, freshness, distribution_anomaly         │
│  Output: 3 Delta tables (overwrite/merge strategy)                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Validation Engine Architecture

```
┌────────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Rule Config       │     │ Validation Engine │     │  ValidationResult│
│  (Dataclass)       │────►│                  │────►│  (Dataclass)     │
│                    │     │  1. Load configs  │     │                  │
│  • rule_id         │     │  2. Execute rules │     │  • rule_id       │
│  • rule_version    │     │  3. Collect results│    │  • passed        │
│  • severity        │     │  4. Route records │     │  • failed_count  │
│  • column_name     │     │  5. Emit metrics  │     │  • failure_rate  │
│  • threshold       │     │                  │     │  • failed_df     │
│  • enabled         │     └──────────────────┘     │  • metadata      │
└────────────────────┘              │                └──────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         ▼                     ▼
                ┌─────────────────┐   ┌─────────────────┐
                │ Quarantine Mgr  │   │ Metrics Collector│
                │                 │   │                 │
                │ Routes failed   │   │ Emits to Delta  │
                │ records to      │   │ observability   │
                │ quarantine      │   │ table           │
                │ Delta table     │   │ (append-only)   │
                └─────────────────┘   └─────────────────┘
```

## Contract Enforcement Model

```
Data Contract = Schema Contract + Quality Contract + SLA Contract

Schema Contract:
  ├── Required columns and types
  ├── Nullable vs non-nullable
  └── Schema enforcement vs evolution per layer

Quality Contract:
  ├── Minimum pass rate per rule (e.g., not_null ≥ 95%)
  ├── Zero tolerance rules (e.g., unique_key = 100%)
  └── Distribution baselines

SLA Contract:
  ├── Freshness threshold (e.g., max 2 hours old)
  ├── Row count bounds (e.g., ±20% of historical mean)
  └── Processing time budget
```

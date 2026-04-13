# Scale Reasoning Summary

## Overview

This document analyzes how every component of the DQ framework behaves across three scale tiers:
- **10K rows**: Development/demo scale
- **10M rows**: Medium production scale
- **1B rows**: Large-scale enterprise production

---

## Rule-Level Scale Analysis

### 1. Not Null Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1s | Negligible | Single filter + count |
| 10M | ~5s | Negligible | Map-only, no shuffle |
| 1B | ~30s | Negligible | Embarrassingly parallel across partitions |

**Optimization at 1B**: None needed. Filter + count is O(n) with no shuffle. This scales linearly and perfectly.

### 2. Unique Key Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1s | <100MB | GroupBy triggers small shuffle |
| 10M | ~10s | ~2GB | Full shuffle to group keys |
| 1B | ~2min | ~20GB | Shuffle-heavy; skewed keys are dangerous |

**Optimization at 1B**: 
- Enable AQE (Adaptive Query Execution) — auto-handles skewed partitions
- If a single key has millions of duplicates (extreme skew), pre-filter with `approx_count_distinct`
- Consider salting: append random suffix to key, aggregate twice

### 3. Accepted Values Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1s | Negligible | isin() with <100 values |
| 10M | ~3s | Negligible | isin() broadcast is tiny |
| 1B | ~20s | <1GB | Switch to broadcast join at >1000 values |

**Optimization at 1B**: For large accepted value sets (>1000), replace `isin()` with a broadcast join against a reference DataFrame. Spark broadcasts the reference to all executors — no shuffle.

### 4. Positive Numeric Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1s | Negligible | Simple filter |
| 10M | ~3s | Negligible | Map-only |
| 1B | ~25s | Negligible | Identical to not_null_check |

**Optimization at 1B**: None needed.

### 5. Timestamp Freshness Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1s | Negligible | Single MAX() aggregate |
| 10M | <1s | Negligible | Delta stores MAX in metadata |
| 1B | ~1s | Negligible | Delta statistics enable O(1) MAX |

**Optimization at 1B**: Delta Lake stores column statistics (min, max, count) in the transaction log. `MAX(timestamp)` can be resolved from metadata without scanning data. This is why Delta >> Parquet for this check.

### 6. Duplicate Detection
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | ~2s | <200MB | Window function with small shuffle |
| 10M | ~15s | ~3GB | Window function = full shuffle by partition key |
| 1B | ~3min | ~30GB | Expensive; dominated by shuffle |

**Optimization at 1B**:
- Use `dropDuplicates()` if you don't need deterministic keep-first/last (no window)
- If deterministic: partition the data by date first, deduplicate per partition
- Pre-filter: count per key, only apply window to keys with count > 1

### 7. Schema Drift Detection
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | <1ms | Negligible | Metadata comparison only |
| 10M | <1ms | Negligible | Same — no data scan |
| 1B | <1ms | Negligible | Schema is in DataFrame metadata |

**Optimization at 1B**: None needed. Schema drift detection is O(1). It compares Python objects, never touches the data.

### 8. Referential Integrity Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K child + 100 parent | <1s | <100MB | Broadcast join |
| 10M child + 10K parent | ~5s | ~500MB | Broadcast join (parent fits in memory) |
| 1B child + 10M parent | ~2min | ~10GB | Shuffle join if parent > memory |

**Optimization at 1B**:
- Broadcast parent table if < 1GB (`spark.sql.autoBroadcastJoinThreshold`)
- If parent > 1GB: partition both tables on the join key, use sort-merge join
- Pre-compute: maintain a Bloom filter of valid parent keys for instant lookup

### 9. Row Count Anomaly Detection
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| Any | <1s | Negligible | Single count + Python arithmetic |

**Optimization at 1B**: Delta stores row count in metadata. `df.count()` on a Delta table is O(1) — it reads the transaction log, not the data.

### 10. Distribution Anomaly Check
| Scale | Time | Memory | Notes |
|-------|------|--------|-------|
| 10K | ~2s | <100MB | Exact quantiles on small data |
| 10M | ~10s | ~1GB | approxQuantile with ε=0.01 |
| 1B | ~1min | ~5GB | approxQuantile or sampled statistics |

**Optimization at 1B**:
- Use `approxQuantile()` with relative error = 0.01 (1%)
  - Uses Greenwald-Khanna algorithm: O(n) time, O(1/ε) space
  - At ε=0.01: O(100) space regardless of data size
- Alternative: compute statistics on a 1% random sample
  - `df.sample(0.01)` → compute on ~10M rows instead of 1B
  - Theoretical accuracy loss < 0.5% for most distributions

---

## Pipeline-Level Scale Analysis

### Bronze Pipeline
| Scale | Total Time | Bottleneck |
|-------|-----------|------------|
| 10K | ~5s | Spark startup |
| 10M | ~30s | Delta write I/O |
| 1B | ~5min | Write + schema enforcement |

**Optimization at 1B**:
- Write partitioned by ingestion date
- Use OPTIMIZE + ZORDER post-write for downstream query performance
- Consider file size tuning: `spark.sql.files.maxRecordsPerFile = 1000000`

### Silver Pipeline
| Scale | Total Time | Bottleneck |
|-------|-----------|------------|
| 10K | ~10s | Deduplication window |
| 10M | ~45s | Deduplication + ref integrity join |
| 1B | ~8min | Shuffle for dedup + joins |

**Optimization at 1B**:
- Incremental processing: only process new Bronze records (not full recompute)
- Use Delta MERGE for upsert instead of full overwrite
- Partition Silver tables by order_date for downstream pruning

### Gold Pipeline
| Scale | Total Time | Bottleneck |
|-------|-----------|------------|
| 10K | ~5s | Aggregation |
| 10M | ~20s | GroupBy aggregation |
| 1B | ~3min | GroupBy + window for ranking |

**Optimization at 1B**:
- Pre-aggregate in Silver: maintain running totals updated via MERGE
- Partition Gold tables by date for incremental updates
- Use materialize views (Delta Live Tables) for automatic refresh

---

## Observability Scale

### Metrics Table Growth
| Pipeline Frequency | Rules | Metrics/Run | Daily Rows | Annual Rows |
|---|---|---|---|---|
| Daily | 10 | ~50 | ~50 | ~18K |
| Hourly | 20 | ~100 | ~2.4K | ~876K |
| Every 15 min | 50 | ~250 | ~24K | ~8.7M |
| Every minute | 100 | ~500 | ~720K | ~263M |

**Optimization**: 
- Partition by `run_timestamp` (date-truncated) for time-range queries
- VACUUM after 90 days to reclaim storage
- For >100M rows/year: aggregate old data into daily summaries

---

## Key Scale Principles

1. **Avoid shuffles where possible**: Filter, map, and aggregate operations are cheap. Joins and window functions trigger shuffles — they dominate costs at scale.

2. **Use Delta metadata**: Delta stores statistics (min, max, count, null count) in the transaction log. Operations like `COUNT(*)`, `MAX(timestamp)`, and predicate pushdown can use metadata without data scans.

3. **Broadcast small tables**: Reference tables < 1GB should be broadcast to avoid shuffle joins. Spark does this automatically below `autoBroadcastJoinThreshold`.

4. **Use approximate algorithms**: At 1B rows, exact percentiles = O(n log n). Approximate = O(n). The difference is 20 minutes vs 2 minutes. Accept 1% error for 10x speedup.

5. **Partition wisely**: Over-partitioning creates small file problems. Under-partitioning forces full scans. Rule of thumb: each partition should have 128MB-1GB of data.

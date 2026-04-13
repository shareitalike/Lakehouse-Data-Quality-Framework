# Anti-Patterns in Production Data Quality

## Overview

These 9 anti-patterns look reasonable in development but fail catastrophically in production. For each, we explain: **what it is**, **why it seems correct**, **how it fails**, and **what to do instead**.

---

## 1. Failing Pipelines on First Bad Record

### What It Looks Like
```python
# ANTI-PATTERN
if df.filter(col("customer_id").isNull()).count() > 0:
    raise Exception("Null customer_id found! Aborting pipeline.")
```

### Why It Seems Correct
"Data quality is important — bad data should never make it through."

### How It Fails in Production
- A single null in 10M records crashes the entire pipeline
- Downstream consumers get NO data instead of 99.99% good data
- Pipeline retries create cascading failures in dependent jobs
- The on-call engineer gets paged at 3 AM for one bad record
- If the bad record comes from a legitimate edge case (new customer type), the pipeline never recovers without code changes

### What To Do Instead
**Quarantine and continue.** Route bad records to a quarantine table. Let the pipeline process the 99.99% that's valid. Alert on quarantine growth rate, not individual failures.

```python
# CORRECT PATTERN
failed_df = df.filter(col("customer_id").isNull())
clean_df = df.filter(col("customer_id").isNotNull())
failed_df.write.mode("append").save(quarantine_path)  # Preserve bad records
clean_df.write.save(output_path)  # Process good records
```

---

## 2. Schema Checks That Prevent Schema Evolution

### What It Looks Like
```python
# ANTI-PATTERN
if df.schema != EXPECTED_SCHEMA:
    raise Exception(f"Schema mismatch! Expected {EXPECTED_SCHEMA}, got {df.schema}")
```

### Why It Seems Correct
"Schema enforcement protects downstream consumers from unexpected changes."

### How It Fails in Production
- Upstream adds a legitimate new column → pipeline crashes
- The fix requires a code deploy → 24-hour data delay
- Every schema change requires coordination between 3 teams
- Eventually teams stop adding useful columns because "it breaks the pipeline"
- Results in a rigid system that can't evolve with business needs

### What To Do Instead
**Separate enforcement from detection.** Bronze enforces (reject unknown schemas). Silver+ detects (alert on changes but accept them with `mergeSchema=True`). Update contracts through a review process, not emergency fixes.

---

## 3. Relying Only on COUNT(*) Validation

### What It Looks Like
```python
# ANTI-PATTERN
current_count = df.count()
if current_count > 0:
    print("Validation passed! Data looks good.")
```

### Why It Seems Correct
"If we have rows, the pipeline is working."

### How It Fails in Production
- 1M rows could be 1M identical duplicates — COUNT is fine, data is garbage
- A table with 999,999 rows (normally 1M) lost 1 row — something is wrong but COUNT says "good enough"
- All values in `revenue` column could be NULL — COUNT counts the rows, not the data
- Distribution could be completely wrong (all prices $0.01) — COUNT can't see this

### What To Do Instead
**Multi-dimensional validation.** Check count AND uniqueness AND null rates AND distribution. No single metric captures data quality.

---

## 4. Ignoring Distribution Drift

### What It Looks Like
```python
# ANTI-PATTERN
avg_price = df.agg(avg("price")).collect()[0][0]
if 10 < avg_price < 100:
    print("Price looks normal!")
```

### Why It Seems Correct
"If the average is in range, the data is reasonable."

### How It Fails in Production
- Anscombe's quartet: datasets with identical mean but wildly different distributions
- A bot sets 10% of prices to $0.01 → average drops slightly, still "in range"
- Currency conversion applied twice → all EUR prices 100x too high → average is in range because EUR is only 15% of data
- Seasonal shift (Black Friday) vs data pipeline bug — both change the average, but one is expected

### What To Do Instead
**Track percentiles AND z-scores.** Compare P25/P50/P75 across runs. A healthy distribution has stable percentiles. Use z-score for center-of-distribution shift, percentiles for shape changes.

---

## 5. Using UDF-Heavy Validation Logic

### What It Looks Like
```python
# ANTI-PATTERN
@udf(returnType=BooleanType())
def validate_email(email):
    import re
    return bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email))

df.withColumn("is_valid", validate_email(col("email"))).filter(col("is_valid"))
```

### Why It Seems Correct
"Python UDFs give us maximum flexibility for complex validation."

### How It Fails in Production
- UDFs serialize data from JVM → Python → JVM per row — 10-100x slower than native Spark
- UDFs break Spark's Catalyst optimizer — no predicate pushdown, no code generation
- UDFs are single-threaded on each executor — no SIMD vectorization
- At 1B rows: native Spark = 30 seconds, UDF = 30 minutes

### What To Do Instead
**Use native Spark functions.** `F.regexp_extract()`, `F.when()`, `F.isin()` — these run on the JVM, benefit from Catalyst optimization, and support vectorized execution. If you absolutely must use custom logic, use Pandas UDFs (vectorized) instead of scalar UDFs.

---

## 6. Not Versioning Validation Rules

### What It Looks Like
```python
# ANTI-PATTERN
NULL_THRESHOLD = 0.05  # Changed from 0.01 on March 15th
                        # Why? Ask Dave. Dave left the company.
```

### Why It Seems Correct
"Just update the threshold when needed. It's one number."

### How It Fails in Production
- Root cause analysis: "Why did this rule pass on March 14th but fail on March 16th?" → "Because someone changed the threshold." → "Who? When? Why?" → Nobody knows.
- Auditing: "What quality standards were applied to the data we reported to regulators last quarter?" → "Whatever the threshold was at the time." → "What was it?" → "Unclear."
- Rollback: "The new threshold is too strict, revert it." → "To what? The previous value wasn't recorded."

### What To Do Instead
**Version every rule config.** Each rule has a `rule_version` string. When thresholds change, increment the version. Store the version in the observability table. Now you can always answer: "What rules and thresholds were active at any point in time?"

---

## 7. Storing Validation Results in OLTP Databases

### What It Looks Like
```python
# ANTI-PATTERN
import psycopg2
conn = psycopg2.connect("postgresql://...")
cursor = conn.cursor()
for result in validation_results:
    cursor.execute("INSERT INTO results VALUES (%s, %s, ...)", (result.rule_id, result.passed, ...))
conn.commit()
```

### Why It Seems Correct
"PostgreSQL is reliable and supports SQL queries for analysis."

### How It Fails in Production
- Row-by-row INSERT = N round trips to the database for N results
- At 1000 rules × 10 metrics = 10,000 INSERTs per pipeline run → OLTP overhead
- PostgreSQL connection limit (typically 100) = pipeline competes with application queries
- OLTP databases are optimized for point lookups, not analytical queries
- Time-series queries ("trend over 30 days") require sequential scans on OLTP — slow

### What To Do Instead
**Use the same data platform.** Store metrics in a Delta table (or your warehouse). Same technology, no additional infrastructure, optimized for analytical queries. Append-only Delta tables handle millions of metric rows efficiently.

---

## 8. Ignoring Late-Arriving Data

### What It Looks Like
```python
# ANTI-PATTERN
today_orders = df.filter(col("order_date") == current_date())
# Process only today's orders, ignore everything else
```

### Why It Seems Correct
"We process today's data today. Simple and clean."

### How It Fails in Production
- A mobile app user orders offline → order arrives 3 days later → missed by the filter
- Partner API batch delivery → yesterday's orders arrive today at 6 AM → missed
- Timezone issues → "today" in UTC includes "tomorrow" in Asia-Pacific → some orders double-counted, others missed
- Aggregate tables are never corrected for late data → permanent undercount

### What To Do Instead
**Design for late-arriving data.** Use event-time processing (watermarks). Use Delta MERGE to upsert late records into the correct partitions. Set SLAs ("we accept data up to 72 hours late") and reprocess accordingly.

---

## 9. Assuming Bronze Data Is Immutable

### What It Looks Like
```python
# ANTI-PATTERN
# "Bronze data never changes. Read once, process, forget."
bronze_df = spark.read.format("delta").load(bronze_path)
# Process and never look at Bronze again
```

### Why It Seems Correct
"Bronze is the raw layer. Once we write it, it's the truth."

### How It Fails in Production
- Compliance requirement: "Delete all data for customer XYZ (GDPR right to be forgotten)" → Bronze data MUST be mutable for compliance
- Data correction: source system resends corrected data for yesterday → Bronze needs UPDATE capability
- Delta table operations: OPTIMIZE, VACUUM change the physical files (compact, clean up) → old file references break
- Schema evolution: adding audit columns to Bronze requires rewriting existing data

### What To Do Instead
**Bronze is append-mostly, not immutable.** New data is appended. Corrections use Delta MERGE. GDPR deletions use Delta DELETE. Use Delta time travel to see "what Bronze looked like at any point in time." Design downstream pipelines to handle Bronze mutations (incremental processing with change data capture).

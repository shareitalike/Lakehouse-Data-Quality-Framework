# Data Quality + Observability — Interview Learning Guide

---

## Elevator Pitches

### 30-Second Version

"I built a configuration-driven data quality framework for Lakehouse pipelines. It validates data at every Medallion layer — Bronze, Silver, Gold — using 10 pluggable rules including distribution anomaly detection. Bad records get quarantined, not dropped. All metrics flow to an append-only observability table for trend analysis. The framework uses PySpark with Delta Lake, no external tools. It's designed to prevent data incidents before they reach dashboards."

### 2-Minute Version

"I designed and built a production-grade Data Quality and Observability framework for Lakehouse pipelines using PySpark and Delta Lake. The system follows Medallion architecture — Bronze for raw ingestion, Silver for cleaned trusted data, Gold for business aggregations.

The core innovation is the configuration-driven rule engine. Each validation rule — null checks, uniqueness, referential integrity, distribution anomaly detection — is a pure PySpark function driven by strongly-typed dataclass configs. Adding a new check means creating a config object, not writing code. This scales from 10 rules managed by one person to 200 rules managed by a platform team.

Two key design decisions I'm proud of: First, quarantine over failure. Bad records never crash the pipeline — they route to a quarantine Delta table where they can be investigated and reprocessed. Second, the separation of validation from observability. Validation gates data at each layer. Observability tracks metrics in an append-only Delta table for trend analysis. These are different concerns with different lifecycles.

The framework includes distribution anomaly detection using dual z-score and percentile-based methods — because mean comparison alone misses shape changes in data. It's the difference between catching 'the average price shifted' and catching 'prices are now bimodal — some at $5 and some at $500.'

Everything is interview-defensible — every module has inline documentation covering design decisions, trade-offs, failure modes, and scale considerations from 10K to 1 billion rows."

### 5-Minute Version

*Use the 2-minute version, then add:*

"Let me walk you through the architecture. Data enters Bronze as raw events — we accept everything, dropping nothing. At the Bronze boundary, we run schema drift detection and critical null checks. Records failing critical rules go to quarantine — they're preserved for investigation, not lost.

Silver is the trust boundary. We deduplicate using ROW_NUMBER with deterministic tiebreaking — not dropDuplicates, which is non-deterministic. We parse timestamps, validate referential integrity using left anti-joins, and normalize enums. After Silver, every downstream consumer can trust the data.

Gold creates business-facing aggregations — daily revenue, product performance, customer lifetime value. Gold validation includes row count anomaly detection against historical baselines and freshness SLA checks.

The observability layer deserves attention. Every validation run emits structured metrics — null rates, duplicate rates, freshness lag, distribution drift scores — to an append-only Delta table. Append-only is deliberate: it creates an immutable audit trail, enables time-series trend analysis, and prevents accidental metric deletion. I can answer 'what was the null rate 3 months ago?' with a simple SQL query.

For distribution monitoring, I use both z-score and percentile detection. Z-score catches center-of-distribution shifts in bell-curve data. Percentile detection catches shape changes in any distribution. A price manipulation bot might not change the mean much, but it destroys the percentile structure. You need both.

The SQL observability layer follows dbt philosophy — declarative tests that anyone can write and review. Referential integrity checks, uniqueness tests, accepted value validations — all expressed as SQL queries. This makes data quality accessible to analysts, not just engineers.

At scale, I've analyzed every component from 10K to 1B rows. Key insights: avoid shuffles for validation (use map-only operations where possible), leverage Delta metadata for COUNT and MAX (O(1) instead of full scan), use approximate quantiles for distribution analysis at 1B+ rows, and partition observability tables by date for efficient trending queries."

---

## Panel Simulation

### Interviewer 1: Hiring Manager

**Focus: Business impact, team dynamics, production readiness**

---

**Q1: "Why is data quality important enough to build a framework for it?"**

**Strong Answer:** "Data quality issues cost real money. A duplicate order in Gold means the revenue dashboard shows 2x the actual number — the CFO sees wrong metrics, makes wrong decisions. A missing customer_id means attribution models break — marketing spends money on campaigns they can't measure. By the time someone notices, the damage is done. A framework prevents this proactively — it catches issues at ingestion, before they compound through Silver into Gold."

**Weak Answer:** "Because bad data is bad." *(No business impact, no specifics)*

**Follow-up Trap:** *"How do you measure the ROI of a data quality framework?"*

**Strong:** "Three metrics: (1) Mean Time To Detect — how fast do we find issues? Before: 72 hours (someone notices a wrong dashboard). After: <15 minutes (automated alerting). (2) Data Incident Count — production incidents caused by data quality. Track reduction over quarters. (3) Quarantine Rate Trend — if quarantine rate is decreasing, upstream quality is improving."

---

**Q2: "How would you roll this out to a team that doesn't have data quality processes?"**

**Strong Answer:** "Start small, prove value, expand. Week 1: deploy null checks and uniqueness checks on ONE critical table — the one that generates the most complaints. Week 2: show the team the observability dashboard — 'look, your customer table has 3% nulls that nobody knew about.' Week 3: add business-specific rules. Week 4: expand to the next table. Never force adoption — let the value speak."

**Weak Answer:** "Mandate that all teams use the framework from day one." *(Resistance, no buy-in)*

**Follow-up Trap:** *"What if the team says 'we don't have time for data quality'?"*

**Strong:** "That's not a time problem — it's a visibility problem. They're already spending time on data quality: debugging wrong dashboards, answering support tickets, reconciling reports. The framework doesn't add time — it shifts time from reactive firefighting to proactive prevention."

---

**Q3: "Tell me about a time you designed something that had to work at scale."**

**Strong Answer:** *(Use the DQ framework)* "The distribution anomaly detection was designed with three scale tiers. At 10K rows: exact percentiles, instant. At 10M: approximate quantiles using Greenwald-Khanna algorithm — O(n) instead of O(n log n). At 1B: sampled statistics on 1% of data. I chose configurable accuracy because different business contexts tolerate different error bounds — financial data needs exact, product recommendations tolerate 1% error."

**Weak Answer:** "I always write scalable code." *(No specifics)*

---

**Q4: "How do you handle disagreements about data quality thresholds?"**

**Strong Answer:** "Data contracts. The data producer and consumer agree on thresholds in writing: 'customer_id null rate must be <1%, order_id must be unique, data must be <4 hours old.' The contract is versioned and reviewed quarterly. When there's a disagreement, the contract is the source of truth. If someone wants to change a threshold, they propose a contract update with business justification."

**Weak Answer:** "I set the thresholds based on best practices." *(No collaboration, no process)*

**Follow-up Trap:** *"What if the data producer says their data is fine but your rules say otherwise?"*

**Strong:** "The observability dashboard resolves this objectively. I show them the trend: 'Your null rate was 0.5% in January, 2.3% in February, 5.1% in March. Something changed.' Data doesn't lie. We jointly investigate the root cause rather than debating opinions."

---

**Q5: "What would you do differently if you built this again?"**

**Strong Answer:** "Three things: (1) Start with observability before validation. I'd deploy metrics-only for two weeks to understand baseline data patterns before setting any thresholds. Premature thresholds cause false positives. (2) Add anomaly detection earlier — I built it last but it's the most valuable rule. (3) Build a simple web UI for the observability dashboard — SQL queries are powerful but non-engineers need visual trend charts."

**Weak Answer:** "Nothing, I'm happy with the design." *(No self-awareness)*

---

### Interviewer 2: Senior Data Engineer

**Focus: Technical depth, Spark internals, production patterns**

---

**Q1: "Why ROW_NUMBER instead of dropDuplicates?"**

**Strong Answer:** "dropDuplicates() is non-deterministic — it doesn't guarantee WHICH record survives. In production, this means the same input can produce different output across runs. ROW_NUMBER with orderBy gives deterministic deduplication — I always keep the latest record. Additionally, ROW_NUMBER gives me the DUPLICATE records (row_num > 1) for quarantine, while dropDuplicates just silently drops them."

**Weak Answer:** "dropDuplicates is simpler, but ROW_NUMBER gives more control." *(Missing the determinism argument — the critical point)*

**Follow-up Trap:** *"But ROW_NUMBER triggers a full shuffle. How do you handle that at scale?"*

**Strong:** "Three mitigations: (1) AQE handles moderate skew automatically by splitting large partitions. (2) For extreme skew, I pre-filter: count per key, and only apply the window to keys with count > 1. Keys with count = 1 (the vast majority) bypass the window entirely. (3) If the window is still too expensive, I partition the data by date first and deduplicate within each partition — this reduces shuffle size proportionally."

---

**Q2: "Explain why your observability table is append-only."**

**Strong Answer:** "Three reasons: (1) Immutable audit trail — I can always prove what quality checks passed on a specific date. This matters for compliance. (2) Time-series analysis — trend detection requires historical data. If I overwrite, I lose the ability to answer 'is null rate trending upward?' (3) Accidental protection — a misguided DELETE or TRUNCATE on an overwritable table destroys all monitoring history. Append-only prevents this. It's the same principle as a database WAL — append-only for reliability."

**Weak Answer:** "So we don't lose data." *(Too vague, missing the time-series and compliance angles)*

**Follow-up Trap:** *"But append-only means the table grows forever. How do you handle that?"*

**Strong:** "Three strategies: (1) Partition by date for efficient time-range pruning. (2) VACUUM with a 90-day retention policy — old data is removed automatically. (3) For long-term analysis, aggregate old data into daily summaries before vacuuming the detailed records. This gives both detailed recent data AND long-term trends."

---

**Q3: "Why left anti-join for referential integrity instead of NOT IN?"**

**Strong Answer:** "The null trap. If the parent column contains ANY null value, NOT IN returns no results — it's equivalent to NULL IN (1, 2, NULL) which evaluates to NULL, which is falsy. Anti-join handles nulls correctly. Additionally, Spark generates a more efficient physical plan for anti-join — it can use broadcast hash anti-join when the parent table is small, avoiding a full shuffle."

**Weak Answer:** "Anti-join is more Spark-idiomatic." *(Misses the null trap — the critical point)*

**Follow-up Trap:** *"What if the parent table has 100 million rows?"*

**Strong:** "Then broadcast won't work — the table doesn't fit in memory. I'd use sort-merge anti-join with both tables partitioned on the join key. If the parent table is already Delta with Z-ordering on the ID column, Spark can use file-level statistics to skip entire files whose min/max ranges don't overlap with the child keys."

---

**Q4: "How does your distribution anomaly check handle non-normal distributions?"**

**Strong Answer:** "That's exactly why I use dual detection. Z-score assumes normality — it fails for right-skewed data like prices and quantities. Percentile detection is non-parametric — it works for any distribution shape. By comparing P25/P50/P75 bands against historical baselines, I detect shape changes regardless of the underlying distribution. Real-world data is almost never normal — prices follow log-normal, quantities follow Poisson-like, revenue follows power-law. Percentiles handle all of these."

**Weak Answer:** "I check if the mean is within a threshold." *(Shows they don't understand the problem)*

**Follow-up Trap:** *"How do you compute percentiles at 1 billion rows?"*

**Strong:** "approxQuantile with the Greenwald-Khanna algorithm. O(n) time, O(1/ε) space. At ε=0.01 (1% relative error), this uses about 100 quantile summaries regardless of data size. On 1B rows, exact percentiles take ~20 minutes (requires sorting). Approximate percentiles take ~2 minutes. For financial data where precision matters, I'd use exact quantiles on a stratified sample — sample 1% of data per category, compute exact percentiles on the sample."

---

**Q5: "Walk me through what happens when a Silver write fails mid-execution."**

**Strong Answer:** "Delta Lake's ACID guarantees handle this. A Delta write is atomic — if it fails mid-execution, no files are committed to the transaction log. The table remains in its pre-write state. No partial data, no corruption. On retry, the pipeline rereads Bronze (which is intact) and recomputes Silver from scratch. This is idempotent by design — the same input always produces the same output, regardless of how many times you retry."

**Weak Answer:** "We'd need to clean up the bad data manually." *(Shows they don't understand Delta ACID)*

**Follow-up Trap:** *"What if the write succeeds but the data is wrong?"*

**Strong:** "Delta time travel. If we discover the data is wrong within the retention period, we can read the previous version: `spark.read.option('versionAsOf', N-1).load()`. Then we fix the pipeline, reprocess from Bronze, and overwrite Silver. If the wrong data already propagated to Gold, we recompute Gold as well. This is why Gold is a full recompute from Silver, not incremental — it makes recovery deterministic."

---

### Interviewer 3: Data Platform Architect

**Focus: Architecture decisions, system design, long-term thinking**

---

**Q1: "Why did you separate validation from observability?"**

**Strong Answer:** "Different concerns, different lifecycles, different consumers. Validation is a checkpoint — it decides if data passes or fails. Observability is a time-series — it tracks metrics over time. Validation rules change when business logic changes (quarterly). Observability metrics are immutable and grow forever. Validation serves the pipeline (block/allow data). Observability serves the platform team (trend analysis, capacity planning, SLA monitoring). Mixing them creates a system that's neither a good gate nor a good monitor."

**Weak Answer:** "For separation of concerns." *(Too abstract, no concrete reasoning)*

**Follow-up Trap:** *"Couldn't you achieve both with a single system?"*

**Strong:** "You could, but it optimizes for nothing. A validation system needs low-latency, synchronous execution — check the data and decide NOW. An observability system needs historical storage, async aggregation, and trend analysis. A single system either blocks the pipeline waiting for metrics storage (bad for validation latency) or doesn't store metrics reliably (bad for observability). The separation allows each to be optimized independently."

---

**Q2: "How would this framework evolve if the company scaled to 100 data pipelines?"**

**Strong Answer:** "Four evolution steps: (1) Rule configs move from Python files to a central metadata store (Delta table or database) — platform team manages rules via API, not code commits. (2) Add a rule marketplace — teams can share and reuse rules across pipelines instead of reinventing the same null check. (3) Observability dashboard becomes a web service with role-based access — data engineers see technical metrics, data stewards see business quality KPIs. (4) Add automated rule tuning: analyze historical false positive rates and suggest threshold adjustments. The current framework is the foundation — same rule engine, same metrics schema, different scale of management."

**Weak Answer:** "Just deploy it to all 100 pipelines." *(No architectural thinking)*

**Follow-up Trap:** *"What about rule conflicts between teams?"*

**Strong:** "Data contract governance. Each dataset has an owner. The owner defines and approves the contract. Downstream consumers can ADD rules (stricter checks for their use case) but can't OVERRIDE the owner's rules. This is an access control problem, not a technical problem. The framework supports it via rule_id namespacing: `{team}.{dataset}.{rule_name}`."

---

**Q3: "Why Dataclasses instead of a database for rule configuration?"**

**Strong Answer:** "At current scale (10-50 rules), dataclasses are superior: type-safe, IDE-supported, version-controlled in Git, and reviewable in PRs. A database adds operational complexity (backups, migrations, access control) that isn't justified yet. However, I designed the dataclass structure to be serializable to JSON — when we need a database, the migration is straightforward: serialize dataclasses → store as JSON rows → build API on top. The framework doesn't care where configs come from, only that they're typed correctly."

**Weak Answer:** "Dataclasses are simpler." *(No forward-thinking)*

**Follow-up Trap:** *"Won't that require a code deploy every time someone changes a threshold?"*

**Strong:** "Yes, and that's intentional at this stage. Rule changes SHOULD be reviewed in PRs — an incorrect threshold can quarantine all data or let bad data through. At 100+ rules with a dedicated platform team, we'd move to runtime configuration with change audit logging. But even then, I'd keep critical rules (uniqueness, not-null on keys) in code — they should NEVER be changed without review."

---

**Q4: "How would you handle multi-tenant data quality?"**

**Strong Answer:** "Namespace everything. Rule IDs: `{tenant}.{dataset}.{rule}`. Observability metrics: tagged with `tenant_id`. Quarantine tables: partitioned by tenant. SLAs: per-tenant configuration because different tenants have different quality expectations. The rule engine doesn't change — it operates on DataFrames regardless of tenant. The configuration layer adds tenant isolation. Biggest risk: one tenant's bad data affecting another tenant's metrics. Mitigation: separate observability tables per tenant, or strict WHERE clauses in queries."

**Weak Answer:** "Add a tenant_id column." *(Insufficient — doesn't address configuration, isolation, or SLAs)*

**Follow-up Trap:** *"What if tenants have conflicting schema requirements?"*

**Strong:** "Each tenant gets their own data contract. Tenant A's Bronze might expect 8 columns while Tenant B's expects 12. The schema drift detector uses the tenant-specific contract as baseline. In practice, this means a `contracts/` directory organized by tenant, each with its own schema definition and quality thresholds."

---

**Q5: "If you could only monitor one metric across all pipelines, what would it be?"**

**Strong Answer:** "Freshness. If data isn't fresh, nothing else matters. Null rate, uniqueness, distribution — all irrelevant if the data is 3 days old. Freshness is the canary in the coal mine: if freshness degrades, it means the pipeline is slow, failing, or stuck. It's also the easiest to monitor — single MAX(timestamp) comparison — and the hardest to recover from (stale data has already been consumed by dashboards). Every other data quality issue can be fixed retroactively. Staleness can't."

**Weak Answer:** "Row count." *(Row count tells you volume, not quality)*

**Follow-up Trap:** *"But freshness doesn't detect data corruption."*

**Strong:** "Correct — that's why I said 'if I could only pick ONE.' In practice, you need a layered approach: freshness as the first alarm, then row count anomaly as the second, then distribution analysis as the third. Think of it as severity tiers: stale data > missing data > wrong data. Each tier catches different problems."

---

## Resume Bullet Points

### Primary Bullet (for the DQ Framework project)

> **Designed and built a configuration-driven Data Quality + Observability framework for Lakehouse pipelines** using PySpark and Delta Lake, implementing 10 pluggable validation rules (null checks, uniqueness, distribution anomaly detection) across Medallion architecture layers (Bronze/Silver/Gold). Reduced hypothetical Mean Time To Detect for data quality incidents from 72 hours to <15 minutes through automated observability with append-only metrics trending.

### Technical Depth Bullets

> **Engineered dual-method distribution anomaly detection** combining z-score (parametric) and percentile-band (non-parametric) analysis, detecting data drift patterns (price manipulation, currency bugs, seasonal shifts) invisible to mean-only comparison approaches. Designed for 1B-row scale using Greenwald-Khanna approximate quantiles (10x faster than exact computation).

> **Implemented quarantine-over-failure pattern** routing invalid records to Delta quarantine tables (append-only, metadata-tagged) instead of pipeline crashes, achieving zero-downtime data processing with full auditability. Quarantine rate monitoring enabled root-cause trending with data producers.

> **Built config-driven rule engine** using Python dataclasses with type-safe, versioned rule configurations, enabling non-engineers to manage 50+ validation rules without code changes. Designed for migration from file-based to database-backed configuration at enterprise scale.

### System Scale Bullets

> **Designed validation architecture for 10K→1B row scalability**: map-only operations for null/positive checks (no shuffle), broadcast joins for referential integrity (10K parent table), approximate quantiles for distribution analysis (O(n) vs O(n log n)), and Delta metadata queries for O(1) row counts and freshness checks.

> **Established data contract framework** defining schema, quality, and SLA expectations per Medallion layer, with versioned contracts enabling audit-grade traceability of quality thresholds across historical pipeline runs.

### Impact Metrics (adapt to your context)

> Processing **10K+ order events** through 3-layer validation pipeline with **10 rule types**, catching **~15% injected data issues** (null keys, duplicates, schema drift, distribution anomalies) before reaching business-facing Gold tables.

---

## Quick Reference: Top 10 Interview Questions & One-Line Answers

| # | Question | One-Line Answer |
|---|----------|----------------|
| 1 | Why quarantine vs fail-fast? | "One bad record shouldn't stop 10M good records from being processed." |
| 2 | Why append-only observability? | "Immutable audit trail + time-series trending + accidental deletion prevention." |
| 3 | Why anti-join vs NOT IN? | "NOT IN breaks with nulls — one null parent row returns zero results." |
| 4 | Why ROW_NUMBER vs dropDuplicates? | "Deterministic — I control which record survives." |
| 5 | Why dataclasses vs dicts? | "Type safety, IDE support, self-documenting, validation at construction." |
| 6 | Why separate config from logic? | "Adding a rule = adding a config object, not writing code." |
| 7 | Why both z-score and percentiles? | "Z-score catches center shifts, percentiles catch shape changes." |
| 8 | Why Bronze = schema enforcement? | "Keep garbage out of the lakehouse — controlled entry point." |
| 9 | Why Silver = schema evolution? | "Allow legitimate new columns without pipeline changes." |
| 10 | How to scale to 1B rows? | "Avoid shuffles, use Delta metadata, approximate quantiles, broadcast small tables." |

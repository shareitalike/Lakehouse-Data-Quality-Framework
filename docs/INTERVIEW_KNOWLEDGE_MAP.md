# Interview Knowledge Map: Lakehouse Data Quality Framework

Use this document to prioritize your study. You do not need to be a Python expert; you need to be a **Data Architect** who understands these concepts.

---

## 🟥 Phase 1: The "Must-Know" (90% of Questions)

### 1. Medallion Architecture (The "3-Layer" Story)
*   **Question:** "Why did you use three layers instead of just one?"
*   **Your Answer:** "It separates concerns. **Bronze** preserves the raw history for audit. **Silver** provides a single version of truth where data is cleaned and deduplicated. **Gold** is optimized for business reporting. This modular approach prevents 'Spaghetti Architecture' and makes it easier to find bugs."
*   **Study File:** [pipelines/orchestrator.py](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/pipelines/orchestrator.py)

### 2. Data Quality & Quarantining (The "Self-Healing" Story)
*   **Question:** "What happens when you get a 'null' ID or a negative price?"
*   **Your Answer:** "I avoid 'Stop-and-Fix' pipelines. Instead, I built a **Quarantine Manager**. Bad records are tagged with failure metadata and moved to a separate Delta table. This keeps the production pipeline 'Self-Healing'—the good data continues to the dashboard, and the bad data is isolated for investigation."
*   **Study File:** [docs/SELF_HEALING_ARCHITECTURE.md](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/docs/SELF_HEALING_ARCHITECTURE.md)

### 3. Delta Lake vs. Parquet
*   **Question:** "Why use Delta Lake for this project?"
*   **Your Answer:** "Delta gives us **ACID transactions** and **Schema Enforcement**. In previous roles, a failed job would leave partial data (corrupting the lake). Delta prevents this. I also used **Time Travel** to debug historical issues and **MERGE** for idempotent updates in the Silver layer."

---

## 🟨 Phase 2: The "Technical Deep Dive" (For Senior Interviews)

### 4. Configuration-Driven Development
*   **Question:** "How do you add a new data quality rule?"
*   **Your Answer:** "I don't change the engine code. I add a new entry to the `rule_configs.py`. The framework is **metadata-driven**, which makes it highly scalable for new datasets without rewriting the core pipelines."
*   **Study File:** [config/rule_configs.py](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/config/rule_configs.py)

### 5. Environment Portability (Developer Experience)
*   **Question:** "How did you manage local development vs. production?"
*   **Your Answer:** "I strictly used **Virtual Environments** and an abstraction layer for paths. This allowed me to simulate a full Databricks environment on my local machine's **F: drive** using Spark 4.1.1. This ensures that the code that works on my laptop is guaranteed to work in the Cloud."
*   **Study File:** [utils/path_resolver.py](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/utils/path_resolver.py)

---

## 🟩 Phase 3: The "Soft Skills" (The Career Gap)

### 6. Bridging from Oracle DBA to Data Engineer
*   **Question:** "How does your DBA background help you in Spark?"
*   **Your Answer:** "Spark is just a distributed database. My deep understanding of **Query Optimization, Partitioning, and Data Integrity** from my DBA years translates perfectly to **Shuffle Optimization, Z-Ordering, and Delta Constraints**."
*   **Study File:** [docs/ORACLE_REFRESHER_FOR_DE.md](file:///f:/pyspark_study/Project%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/Pro%20Lakehouse%20Data%20Quality%20and%20Observability%20Framework/docs/ORACLE_REFRESHER_FOR_DE.md)

---

## 🏆 Final Exam: Can you explain these?
1. **Idempotency**: Why can you run your pipeline 10 times and get the same result?
2. **Quarantine vs. Reject**: Why do you save bad data instead of failing the job?
3. **Medallion**: What is the "Single Version of Truth"? (Silver).
4. **Schema Enforcement**: How does Delta handle a new column arriving in Bronze?

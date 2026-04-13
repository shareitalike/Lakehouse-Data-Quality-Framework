# Project Code Walkthrough: Logical Reading Order

This guide provides a step-by-step path to understanding the framework, from high-level orchestration to deep technical rules.

---

## 🏗️ 1. The Entry Point: Orchestrator
**File:** `pipelines/orchestrator.py`

Start here. This is the "Master Script" that runs everything. It defines the flow from Bronze to Silver to Gold.
- **Concepts to see:** Run IDs, Layer sequencing, and the `inject_issues` toggle.

## 📋 2. The Blueprint: Configuration
**Files:** `config/pipeline_configs.py` and `config/rule_configs.py`

Before logic, understand the schema. These files define how rules are parameterized using Python Dataclasses.
- **Concepts to see:** Type-safe configs, default paths, and severity levels.

## 🛠️ 3. The Core Logic: Rule Registry
**File:** `rules/rule_registry.py`

This is the most important architectural piece. It connects static configurations to executable PySpark functions.
- **Concepts to see:** How the registry loops through rules and aggregates results into a final health report.

## 🧪 4. The Chaos Lab: Fault Injection
**Files:** `data_generation/bronze_generator.py` and `data_generation/issue_injector.py`

This is where "Bad Data" is born. We use it to test if our rules actually work.
- **Concepts to see:** How we inject Nulls, Duplicates, and Schema Drift at specific rates.

## 🥈 5. The Trust Boundary: Silver Pipeline
**File:** `pipelines/silver_pipeline.py`

This is the most "Data Engineering" heavy file. It handles deduplication, integrity, and quarantine.
- **Concepts to see:** `anti-joins`, `ROW_NUMBER()`, and routing failed records to Delta tables.

## 📊 6. The Observation Deck: Observability
**Files:** `observability/metrics_collector.py` and `observability/sql_queries.py`

This is how we prove the pipeline is healthy to business users.
- **Concepts to see:** Append-only metrics storage and dbt-style SQL validation queries.

---

### Recommended Learning Steps:
1. **Run a Clean Pass**: Execute the orchestrator with `inject_issues=False`.
2. **Run a Chaos Pass**: Execute with `inject_issues=True` and check the Silver table counts vs the Quarantine table counts.
3. **Trace a Rule**: Pick one rule (e.g., `not_null_check`) and see it move from `config` -> `rule_registry` -> `pipeline`.

# Final Completion Checklist: Lakehouse DQ & Observability

This checklist tracks your final stretch to master the framework and prepare for interviews.

## Phase 1: Deep Dive & Code Walkthrough (Learning)
- [ ] **Core Engine Logic**: Trace a record from `bronze_generator.py` -> `bronze_pipeline.py` -> `silver_pipeline.py`.
- [ ] **Rule Internalization**: Understand the "Why" behind:
    - [ ] `referential_integrity_check.py`: Why `anti-join` and the "Null Trap"?
    - [ ] `duplicate_detection.py`: Why `ROW_NUMBER` vs `dropDuplicates`?
    - [ ] `distribution_anomaly_check.py`: Difference between Z-score and Percentile bands.
- [ ] **Config Mastery**: Explain how `RuleConfig` dataclasses enable "Config-driven" validation without code changes.
- [ ] **Oracle Mapping**: Review `docs/oracle_dba_mapping.md` to bridge Spark logic with DBA experience.

## Phase 2: Operational Mastery (Practicals)
- [ ] **The "Happy Path"**: Run `pipelines/orchestrator.py` with default config and confirm 100% pass rate.
- [ ] **Fault Injection (Level 1)**: Use `issue_injector.py` to inject **Nulls** and **Duplicates**.
    - [ ] Verify records appear in the `quarantine` table.
    - [ ] Verify `observability` metrics reflect the failure rates.
- [ ] **Fault Injection (Level 2)**: Simulate **Schema Drift** using `schema_drift_simulator.py`.
    - [ ] Observe how the `ContractEnforcer` handles unexpected columns.
- [ ] **Fault Injection (Level 3)**: Trigger a **Distribution Drift** (e.g., set all prices to $0.01).
    - [ ] Verify the Z-score/Percentile check catches the anomaly.
- [ ] **Recovery**: Use Delta Time Travel to "reset" a table after a failed/bad run.

## Phase 3: Interview Refinement (Learning)
- [ ] **The Pitch**: Memorize and practice the 30-second and 2-minute elevator pitches in `docs/data_quality_learning_guide.md`.
- [ ] **Scalability Defense**: Be ready to answer: "How does this scale to 1 Billion rows?" (Approximate quantiles, Delta metadata, etc.).
- [ ] **Trade-off Defense**: Be ready to answer: "Why separate Validation from Observability?"
- [ ] **Failure Case Defense**: Be ready to answer: "What happens if a Silver write fails mid-execution?" (Delta ACID/Idempotency).

## Phase 4: Final Polishing
- [ ] **Cleanup**: Clear all temporary `TODO`s and debug `print` statements.
- [ ] **Final Success Report**: Write a 1-page summary of your fault injection results to use as a "Project Portfolio" piece.

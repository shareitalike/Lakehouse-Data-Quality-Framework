# Self-Healing Data Pipelines: Architectural Deep Dive

This document explains how our framework achieves "Self-Healing" and provides word-for-word interview scripts to defend these decisions.

---

## 🏗️ 1. The Core Architecture
Instead of the traditional "Stop on Error" approach, our pipeline uses a **Quarantine Pattern**.

### The Sequence:
1. **Rule Application**: Every record is assigned a boolean flag for every rule (e.g., `is_price_positive`, `is_id_not_null`).
2. **The Decision Point**: 
   - Records meeting **ALL** rules → `Silver Table` (The "Clean" Zone).
   - Records failing **ANY** rule → `Quarantine Table` (The "Hospital").
3. **Healing**: Because the pipeline doesn't crash, the business gets their Gold-layer reports on time for the 99% of clean data. The 1% of bad data is "healed" by moving it out of the way for manual or automated correction.

---

## 🎙️ 2. Word-for-Word Interview Scripts

### Q1: "Why not just use Spark's built-in `dropDuplicates()` or basic filters?"
**Answer:**
> *"Filter-and-drop is a silent killer. If you just drop data, you never know **why** your numbers are shrinking over time. My framework uses a **metadata-rich Quarantine.** Every rejected row is saved with a 'failed_rules' column. This allows us to perform 'Data Forensic' analysis and fix the root cause in the source system rather than just hiding the problem."*

### Q2: "How would you explain 'Self-Healing' to a non-technical manager?"
**Answer:**
> *"Imagine a factory assembly line. If one part is broken, you don't shut down the whole factory and send everyone home (that's a traditional failure). Instead, my system identifies the broken part, puts it in a 'Red Bin' for repair, and keeps the rest of the assembly line moving so the customer still gets their product on time. That is what this Lakehouse framework does for data."*

### Q3: "Coming from a DBA background, how do you handle Spark's 'Lax' schema rules compared to Oracle?"
**Answer:**
> *"In Oracle, the database is the gatekeeper. In a Lakehouse, the **Code** must be the gatekeeper. I leveraged my DBA expertise in constraints and data integrity to build a custom **Validation Layer**. I treat my DQ rules exactly like Oracle 'Check Constraints' or 'Foreign Keys'—ensuring that the data at rest is always high-fidelity."*

---

## 🛠️ 3. The "Chaos Run" Proof (The Logic)
When we set `inject_issues=True`, the following functions handled the "Healing":

1. **`validation_engine.apply_rules()`**:
   - Injected: A negative order total (-$100).
   - Action: The 'positive_price' rule flagged this row as `isValid = False`.
2. **`quarantine_manager.quarantine_records()`**:
   - Target: The failed rows.
   - Action: Written to `delta_output/quarantine/` with the tag `[OrderPriceRangeRule: Failed]`.

---

### The Mastery Tip
If you are asked how you **Repair** the data (the final step of healing), say:
> *"The framework stores quarantine data in Delta format. I have a 'Reprocessing Utility' (conceptually designed) that reads the repair table, applies fix-ups, and merges them back into the Silver layer, completing the healing cycle."*

## 1. Prerequisites
Ensure you have the following installed:
- **Python 3.9+**
- **Java 11 or 17** (Required for Spark)
- **Virtual Environment Setup (F: Drive recommended):**
  ```bash
  python -m venv .venv
  .\.venv\Scripts\pip install pyspark==4.1.1 delta-spark==4.1.0
  ```

---

## 2. Execution Scenarios (Spark 4.1.x Aligned)

### Scenario A: The "Happy Path" (Baseline)
Establish that the system works when data is perfect.
1. Open `pipelines/orchestrator.py`.
2. Ensure `inject_issues=False` in the `if __name__ == "__main__":` block.
3. Run the script using the virtual environment:
   ```powershell
   $env:PYTHONIOENCODING="utf-8"; $env:PYTHONPATH="."; .\.venv\Scripts\python pipelines/orchestrator.py
   ```
4. **Expected Result:** Green ✅ emojis for all layers and 0 records in quarantine tables.

### Scenario B: The "Chaos Path" (Stress Test)
Test the DQ gates by introducing real-world errors.
1. Open `pipelines/orchestrator.py`.
2. Set `inject_issues=True`.
3. Run the script.
4. **Expected Result:** Red ❌ emojis in logs, and specific failure rates (e.g., "5% nulls caught").

---

## 3. Verifying Results (Data Exploration)

After execution, data is stored in the `delta_output/` directory (locally) or `/tmp/lakehouse_dq` (Databricks).

### Check the Quarantine Tables
If data failed a "Critical" rule, it was moved here.
```python
# In a spark session:
df = spark.read.format("delta").load("delta_output/silver/quarantine")
df.show()
```

### Check the Observability Metrics
This table tells the story of your data quality over time.
```python
metrics_df = spark.read.format("delta").load("delta_output/observability/metrics")
metrics_df.sort("execution_timestamp", ascending=False).show()
```

---

## 4. Environment Tips

### Local Execution (Windows/Mac)
- The framework auto-detects your OS and stores data in a folder named `delta_output` in the project root.
- To use an external path, set the environment variable: `FORCE_LOCAL_PATHS=true`.

### Databricks Execution
- Upload the code as a Library or a Folder.
- Use a cluster with **Databricks Runtime 13.x+**.
- Data will be stored in `/tmp/lakehouse_dq` by default.

---

## 5. Troubleshooting
- **Memory Errors:** If running on a laptop, Spark might OOM. Reduce `num_records` in `config/pipeline_configs.py` from 10,000 to 1,000.
- **Delta Errors:** Ensure no other process is holding a lock on the `delta_output` folders. If stuck, simply delete the `delta_output` folder and rerun.

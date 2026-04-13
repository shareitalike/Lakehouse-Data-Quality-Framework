# Databricks Community Edition (CE) Run Book

This guide explains how to migrate your local "Laptop" project to the Databricks Cloud. This is the final step to becoming an interview-ready Senior Lakehouse Architect.

---

## 🛠️ Step 1: Set Up Databricks Community Edition
1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com).
2. If you don't have an account, sign up. (Ensure you select **Community Edition**, not the AWS/Azure free trial which requires a credit card).
3. Log in to your workspace.

---

## 📦 Step 2: Prepare Your Code
Because Databricks CE is a cloud environment, we need to upload your local files.

### Option A: Manual Upload (Easiest for Now)
1. Zip your project folders (`pipelines`, `engine`, `config`, `utils`, `rules`, `schemas`, `data_generation`, `observability`).
2. Do **NOT** include the `.venv` or `delta_output` folders (they are strictly for local use).
3. In Databricks, go to **Workspace** -> **Users** -> **[Your Email]**.
4. Right-click and select **Import** -> **Upload the Zip file**.

---

## ⚙️ Step 3: Configure the Cluster
1. Go to **Compute** -> **Create Compute**.
2. Name it `Lakehouse_DQ_Cluster`.
3. Databricks Runtime Version: Select **14.3 LTS** or higher (this aligns with our Spark 4.1.x logic).
4. **IMPORTANT**: Go to the **Libraries** tab of your cluster.
   - Click **Install New**.
   - Select **PyPI**.
   - Enter `delta-spark==3.0.0` (Databricks 14.3 uses Delta 3.x internals, our code is compatible).
5. Start the cluster.

---

## 📓 Step 4: Create the Execution Notebook
Create a new Notebook called `Orchestrator_Main` and paste the following code:

```python
# 1. Verify our environment
import os
print(f"Running on Databricks Runtime: {os.environ.get('DATABRICKS_RUNTIME_VERSION')}")

# 2. Add the uploaded folders to the system path
import sys
sys.path.append("/Workspace/Users/[YOUR_EMAIL]/Pro Lakehouse Data Quality and Observability Framework")

# 3. Execute the pipeline
from pipelines.orchestrator import run_full_pipeline
from config.pipeline_configs import PipelineConfig

config = PipelineConfig.default()
results = run_full_pipeline(config, inject_issues=True)

# 4. View results
print(f"Master Run ID: {results['master_run_id']}")
```

---

## 📊 Step 5: The "Mic Drop" (Visualizing Data)
Once the pipeline runs, your data will be saved in `/tmp/lakehouse_dq` (as defined in our `path_resolver.py`). 

To show this off in an interview, run this in a new cell:
```python
%sql
SELECT * FROM delta.`/tmp/lakehouse_dq/silver/quarantine`
```

---

## 🏆 Interview Cheat Sheet: The Cloud Deployment
When they ask how you handled the cloud:
1. *"I managed a Single Node cluster on Databricks CE."*
2. *"I configured cluster libraries for Delta Spark support."*
3. *"I leveraged the DBFS (Databricks File System) for the Medallion layers."*
4. *"I built a SQL Dashboard over the Delta tables for DQ observability."*

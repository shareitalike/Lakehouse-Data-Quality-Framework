# Environment Upgrade Log: Spark 4.1.1 Alignment

This log documents every step taken to align the local environment with the global Spark 4.1.1 installation.

## Step 1: Upgrading Python Packages
**Goal:** Align PySpark and Delta-Spark library versions.

**Command:**
```powershell
pip install pyspark==4.1.1 delta-spark==4.1.0
```

**Result:** [Retry Needed] (File lock error encountered)

## Step 2: System Cleanup (Process Kill)
**Goal:** Clear file locks on PySpark JARs.

**Command:**
```powershell
Get-Process | Where-Object { $_.Name -match "python" -or $_.Name -match "java" } | Stop-Process -Force
```

**Result:** [Completed] Successfully cleared active sessions.

## Step 3: Pivot to F: Drive (Virtual Environment)
**Goal:** Solve "No Space Left on Device" on C: and isolate the project.

**Reasoning:** C: drive has <1.5GB free. F: drive has 26GB free. Creating an isolated environment on F: is the most stable path forward.

**Command:**
```powershell
python -m venv .venv
```

**Result:** [Completed] Successfully created isolated environment.

## Step 4: Installing Aligned Packages in .venv
**Goal:** Finalize the 4.1.x environment alignment.

**Command:**
```powershell
.\.venv\Scripts\pip install pyspark==4.1.1 delta-spark==4.1.0
```

**Result:** [Completed] Successfully installed Spark 4.1.1 and Delta 4.1.0.

## Step 5: Verification Run
**Goal:** Confirm the "Happy Path" baseline using the new environment.

**Command:**
```powershell
$env:PYTHONIOENCODING="utf-8"; $env:PYTHONPATH="."; .\.venv\Scripts\python pipelines/orchestrator.py
```

**Result:** [Completed] Baseline Established!
**Goal:** Ensure Spark uses the correct global paths.

**Paths Checked:**
- `SPARK_HOME`: C:\spark (Spark 4.1.1)
- `HADOOP_HOME`: C:\hadoop (Includes winutils.exe)

## Step 3: Updating Project Code
**Goal:** Refining `utils/path_resolver.py` for Spark 4.x compatibility.

**Changes:**
- Updated JAR package reference to `io.delta:delta-spark_2.13:4.1.0`.

## Step 4: Verification Run
**Goal:** Execute the "Happy Path" baseline.

**Command:**
```powershell
$env:PYTHONIOENCODING="utf-8"; $env:PYTHONPATH="."; python pipelines/orchestrator.py
```

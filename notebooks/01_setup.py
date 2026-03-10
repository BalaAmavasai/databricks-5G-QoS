# Databricks notebook source
# =============================================================================
# Notebook 01 — Environment Setup
# =============================================================================
# Purpose : Create Delta Lake tables, install Python dependencies, and verify
#            the workspace is ready to run the 5G QoS streaming demo.
#
# Run this notebook ONCE before running notebooks 02–05.
#
# Requirements:
#   - Databricks Runtime 13.x or later (includes Delta Lake + Spark 3.4+)
#   - Unity Catalog enabled (or update paths to use legacy Hive metastore)
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 5G QoS Streaming Demo — Environment Setup
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Installs Python dependencies
# MAGIC 2. Configures the Delta Lake storage paths
# MAGIC 3. Creates the bronze / silver / gold Delta tables
# MAGIC 4. Verifies the setup is correct
# MAGIC
# MAGIC **Run this once per cluster restart** (or once if using Unity Catalog
# MAGIC  with persisted tables).

# COMMAND ----------

# MAGIC %md ## 1. Python dependencies

# COMMAND ----------

# PyYAML is needed to read config/config.yaml from the repo
# All other dependencies (PySpark, Delta) are provided by the Databricks runtime.
%pip install pyyaml --quiet

# COMMAND ----------

# Restart Python to pick up newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## 2. Load configuration

# COMMAND ----------

import os
import sys
import yaml
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Resolve the repo root.  Databricks checks out repos to /Workspace/Repos/...
# If running from DBFS or a local path, adjust REPO_ROOT accordingly.
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) \
            if "__file__" in dir() else "/Workspace/Repos/<your-org>/databricks-qos"

CONFIG_PATH = os.path.join(REPO_ROOT, "config", "config.yaml")

with open(CONFIG_PATH, "r") as fh:
    cfg = yaml.safe_load(fh)

paths = cfg["paths"]
print("Configuration loaded:")
for k, v in paths.items():
    print(f"  {k:20s} -> {v}")

# COMMAND ----------

# MAGIC %md ## 3. Add repo source to PYTHONPATH

# COMMAND ----------

src_path = os.path.join(REPO_ROOT, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Verify the import works
from schema import (  # noqa: F401
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_CELL_KPI_SCHEMA,
    GOLD_SLICE_KPI_SCHEMA,
    GOLD_SLA_VIOLATION_SCHEMA,
)
print("Schema module imported successfully.")

# COMMAND ----------

# MAGIC %md ## 4. Create Delta tables

# COMMAND ----------

def _create_delta_table(path: str, schema, partition_cols=None, comment: str = "") -> None:
    """Create an empty Delta table at *path* with *schema* if it does not exist."""
    partition_cols = partition_cols or []

    # Build a zero-row DataFrame with the correct schema and write as Delta
    empty_df = spark.createDataFrame([], schema)

    writer = empty_df.write.format("delta").mode("ignore")  # ignore = don't overwrite
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)

    # Register table in the Hive metastore for SQL access
    table_name = path.rstrip("/").rsplit("/", 1)[-1]
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
        COMMENT '{comment}'
    """)
    print(f"  Table '{table_name}' ready at {path}")


print("Creating Delta tables …")

_create_delta_table(
    paths["bronze"],
    BRONZE_SCHEMA,
    comment="Raw 5G UE QoS events from the simulator (bronze layer)",
)

_create_delta_table(
    paths["silver"],
    SILVER_SCHEMA,
    partition_cols=["slice_type"],
    comment="Validated and enriched 5G UE QoS events (silver layer)",
)

_create_delta_table(
    paths["gold_cell"],
    GOLD_CELL_KPI_SCHEMA,
    partition_cols=["cell_id"],
    comment="Windowed per-cell KPI aggregations (gold layer)",
)

_create_delta_table(
    paths["gold_slice"],
    GOLD_SLICE_KPI_SCHEMA,
    partition_cols=["slice_type"],
    comment="Windowed per-slice KPI aggregations (gold layer)",
)

_create_delta_table(
    paths["gold_sla"],
    GOLD_SLA_VIOLATION_SCHEMA,
    comment="SLA violation events (gold layer)",
)

print("\nAll Delta tables created.")

# COMMAND ----------

# MAGIC %md ## 5. Enable Delta Change Data Feed (required for streaming reads)

# COMMAND ----------

# Change Data Feed lets downstream streaming jobs read only new rows.
for table_path in [paths["bronze"], paths["silver"]]:
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}`
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    print(f"CDF enabled on: {table_path}")

# COMMAND ----------

# MAGIC %md ## 6. Verify setup

# COMMAND ----------

from delta.tables import DeltaTable

print("Table verification:")
for name, path in paths.items():
    if name in ("base", "checkpoints"):
        continue
    dt = DeltaTable.forPath(spark, path)
    version = dt.history(1).select("version").collect()[0][0]
    print(f"  {name:15s}  version={version}  path={path}")

print("\nSetup complete.  Proceed to notebook 02_data_simulator.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC | Notebook | Description |
# MAGIC |----------|-------------|
# MAGIC | `02_data_simulator` | Generate and write simulated 5G events to the bronze Delta table |
# MAGIC | `03_streaming_pipeline` | Structured Streaming bronze → silver → gold |
# MAGIC | `04_qos_analytics` | Interactive QoS KPI analysis and SLA reporting |
# MAGIC | `05_dashboard_queries` | Pre-built SQL queries for Databricks SQL dashboards |

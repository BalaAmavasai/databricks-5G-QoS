# Databricks notebook source
# =============================================================================
# Notebook 02 — 5G QoS Data Simulator
# =============================================================================
# Purpose : Continuously generate simulated 5G UE QoS events and write them
#            to the bronze Delta table, mimicking a real-time data feed.
#
# This notebook should run in a **detached** or **continuous** fashion while
# notebooks 03 and 04 consume the stream.  Stop it with the "Cancel" button
# once the demo is complete.
#
# Architecture
# ─────────────────────────────────────────────────────────────────────────────
#
#   [Simulator loop]
#       │  generate_batch() → list[dict]
#       │  Spark DataFrame (BRONZE_SCHEMA)
#       ▼
#   [Bronze Delta table]   ← append-only
#       │
#       └──> (consumed by Notebook 03 streaming pipeline)
#
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 5G QoS Data Simulator
# MAGIC
# MAGIC Generates realistic 5G User Equipment (UE) Quality-of-Service telemetry
# MAGIC events and appends them to the **bronze** Delta Lake table every few seconds.
# MAGIC
# MAGIC The simulator covers:
# MAGIC - Three network slice types: **eMBB**, **URLLC**, **mMTC**
# MAGIC - 20 simulated cells spread across UK cities
# MAGIC - Configurable anomaly injection (default 5%)

# COMMAND ----------

# MAGIC %md ## 1. Imports and configuration

# COMMAND ----------

import os
import sys
import time
import yaml
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Resolve repo root (adjust if needed for your Databricks Repos path)
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get(
    "QOS_REPO_ROOT",
    "/Workspace/Repos/<your-org>/databricks-qos"
)

sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

with open(os.path.join(REPO_ROOT, "config", "config.yaml")) as fh:
    cfg = yaml.safe_load(fh)

paths      = cfg["paths"]
sim_cfg    = cfg["simulator"]
BRONZE_PATH = paths["bronze"]

# Simulator parameters (override via widget below if desired)
NUM_CELLS          = sim_cfg["num_cells"]
EVENTS_PER_BATCH   = sim_cfg["num_ues_per_cell"] * NUM_CELLS
ANOMALY_RATE       = sim_cfg["anomaly_rate"]
BATCH_INTERVAL_SEC = sim_cfg["batch_interval_sec"]

print(f"Bronze table : {BRONZE_PATH}")
print(f"Cells        : {NUM_CELLS}")
print(f"Events/batch : {EVENTS_PER_BATCH}")
print(f"Anomaly rate : {ANOMALY_RATE:.0%}")
print(f"Batch interval (s): {BATCH_INTERVAL_SEC}")

# COMMAND ----------

# MAGIC %md ## 2. Databricks widgets (optional overrides)

# COMMAND ----------

dbutils.widgets.text("num_batches",      "0",    "Number of batches (0 = infinite)")
dbutils.widgets.text("events_per_batch", str(EVENTS_PER_BATCH), "Events per batch")
dbutils.widgets.text("anomaly_rate",     str(ANOMALY_RATE),     "Anomaly rate (0–1)")
dbutils.widgets.text("batch_interval",   str(BATCH_INTERVAL_SEC), "Batch interval (seconds)")

# Read widget values
_num_batches      = int(dbutils.widgets.get("num_batches"))
_events_per_batch = int(dbutils.widgets.get("events_per_batch"))
_anomaly_rate     = float(dbutils.widgets.get("anomaly_rate"))
_batch_interval   = float(dbutils.widgets.get("batch_interval"))

NUM_BATCHES        = _num_batches if _num_batches > 0 else float("inf")
EVENTS_PER_BATCH   = _events_per_batch
ANOMALY_RATE       = _anomaly_rate
BATCH_INTERVAL_SEC = _batch_interval

print(f"\nEffective settings:")
print(f"  Batches        : {'infinite' if NUM_BATCHES == float('inf') else NUM_BATCHES}")
print(f"  Events/batch   : {EVENTS_PER_BATCH}")
print(f"  Anomaly rate   : {ANOMALY_RATE:.1%}")
print(f"  Interval (s)   : {BATCH_INTERVAL_SEC}")

# COMMAND ----------

# MAGIC %md ## 3. Schema and simulator imports

# COMMAND ----------

from schema import BRONZE_SCHEMA
from qos_simulator import generate_batch

# Quick sanity check — generate one event and print it
sample = generate_batch(num_cells=NUM_CELLS, events_per_batch=1, anomaly_rate=0)
print("\nSample event:")
for k, v in sample[0].items():
    print(f"  {k:25s}: {v}")

# COMMAND ----------

# MAGIC %md ## 4. Simulator loop
# MAGIC
# MAGIC The cell below runs indefinitely (or for `num_batches` iterations).
# MAGIC **Cancel** the cell to stop the simulator.

# COMMAND ----------

batch_num   = 0
total_events = 0
start_time  = time.time()

print(f"Simulator started at {datetime.datetime.utcnow().isoformat()}Z")
print(f"Writing to: {BRONZE_PATH}\n")

while batch_num < NUM_BATCHES:
    batch_num += 1
    ts = datetime.datetime.utcnow()

    # -----------------------------------------------------------------------
    # 1. Generate a batch of simulated events
    # -----------------------------------------------------------------------
    events = generate_batch(
        num_cells=NUM_CELLS,
        events_per_batch=EVENTS_PER_BATCH,
        anomaly_rate=ANOMALY_RATE,
        timestamp=ts,
    )

    # -----------------------------------------------------------------------
    # 2. Convert to Spark DataFrame — enforce the bronze schema
    #    (all values are strings, matching BRONZE_SCHEMA)
    # -----------------------------------------------------------------------
    # Filter out any extra debug keys added by _apply_anomaly
    bronze_keys = {f.name for f in BRONZE_SCHEMA.fields}
    clean_events = [{k: v for k, v in e.items() if k in bronze_keys} for e in events]

    df = spark.createDataFrame(clean_events, schema=BRONZE_SCHEMA)

    # -----------------------------------------------------------------------
    # 3. Append to the bronze Delta table
    # -----------------------------------------------------------------------
    (
        df.write
        .format("delta")
        .mode("append")
        .save(BRONZE_PATH)
    )

    total_events += len(events)
    elapsed       = time.time() - start_time
    rate          = total_events / elapsed if elapsed > 0 else 0

    # Progress report every batch
    anomaly_count = sum(1 for e in events if e.get("is_anomaly") == "true")
    print(
        f"[Batch {batch_num:5d}] {ts.strftime('%H:%M:%S')} | "
        f"events={len(events):4d} | anomalies={anomaly_count:3d} | "
        f"total={total_events:7d} | rate={rate:.1f} ev/s"
    )

    # -----------------------------------------------------------------------
    # 4. Wait before next batch
    # -----------------------------------------------------------------------
    if batch_num < NUM_BATCHES:
        time.sleep(BATCH_INTERVAL_SEC)

print(f"\nSimulator finished. Total events written: {total_events:,}")

# COMMAND ----------

# MAGIC %md ## 5. Quick verification — row count and sample data

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, BRONZE_PATH)
history_df = dt.history(5)
display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# Show the last 20 events
df_bronze = spark.read.format("delta").load(BRONZE_PATH)
display(
    df_bronze.orderBy("event_timestamp", ascending=False).limit(20)
)

# COMMAND ----------

# Anomaly breakdown
from pyspark.sql.functions import col, count, when

anomaly_stats = (
    df_bronze
    .groupBy("slice_type", "is_anomaly")
    .agg(count("*").alias("event_count"))
    .orderBy("slice_type", "is_anomaly")
)
display(anomaly_stats)

# Databricks notebook source
# =============================================================================
# Notebook 03 — Structured Streaming Pipeline
# =============================================================================
# Purpose : Read raw events from the bronze Delta table, validate and enrich
#            them (silver), then compute windowed KPIs and SLA violation events
#            written to the gold Delta tables.
#
# Pipeline architecture
# ─────────────────────────────────────────────────────────────────────────────
#
#  [Bronze Delta]
#      │   readStream (Auto-Loader or Delta CDF)
#      ▼
#  [Bronze → Silver transform]
#      │   • Cast string fields to correct types
#      │   • Derive signal_quality_class from RSRP
#      │   • Compute sla_breach flag
#      │   • Add ingested_at timestamp
#      ▼
#  [Silver Delta]   ──────────────────────────────────────┐
#      │   readStream                                      │  readStream
#      ▼                                                   ▼
#  [Silver → Gold: Cell KPIs]              [Silver → Gold: SLA Violations]
#  [Silver → Gold: Slice KPIs]
#
# Each streaming query runs concurrently in the same notebook.
# Use Trigger.AvailableNow for one-shot batch processing, or
# processingTime for continuous streaming.
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 5G QoS Structured Streaming Pipeline
# MAGIC
# MAGIC **Bronze → Silver → Gold**
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `ue_events` (raw) | Raw events from simulator |
# MAGIC | Silver | `ue_events` (clean) | Validated, typed, enriched |
# MAGIC | Gold | `cell_kpis` | Windowed per-cell KPIs |
# MAGIC | Gold | `slice_kpis` | Windowed per-slice KPIs |
# MAGIC | Gold | `sla_violations` | Per-event SLA breaches |

# COMMAND ----------

# MAGIC %md ## 1. Imports and configuration

# COMMAND ----------

import os
import sys
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, BooleanType, TimestampType
)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Spark tuning for streaming
# ---------------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "8")          # Reduce for demo
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

REPO_ROOT = os.environ.get(
    "QOS_REPO_ROOT",
    "/Workspace/Repos/<your-org>/databricks-qos"
)

sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

with open(os.path.join(REPO_ROOT, "config", "config.yaml")) as fh:
    cfg = yaml.safe_load(fh)

paths          = cfg["paths"]
stream_cfg     = cfg["streaming"]
alert_cfg      = cfg["alerts"]

BRONZE_PATH     = paths["bronze"]
SILVER_PATH     = paths["silver"]
GOLD_CELL_PATH  = paths["gold_cell"]
GOLD_SLICE_PATH = paths["gold_slice"]
GOLD_SLA_PATH   = paths["gold_sla"]
CKPT_BASE       = paths["checkpoints"]

WINDOW_DURATION = stream_cfg["window_duration"]   # e.g. "1 minute"
SLIDE_DURATION  = stream_cfg["slide_duration"]    # e.g. "30 seconds"
WATERMARK_DELAY = stream_cfg["watermark_delay"]   # e.g. "30 seconds"
TRIGGER_INTERVAL = stream_cfg["trigger_interval"] # e.g. "10 seconds"

print(f"Bronze  : {BRONZE_PATH}")
print(f"Silver  : {SILVER_PATH}")
print(f"Gold (cell)  : {GOLD_CELL_PATH}")
print(f"Gold (slice) : {GOLD_SLICE_PATH}")
print(f"Gold (SLA)   : {GOLD_SLA_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. UDFs — signal quality classification and SLA breach detection

# COMMAND ----------

from qos_simulator import classify_signal_quality, check_sla_breach

# Register as Spark UDFs so they can be used in DataFrame transformations
classify_signal_quality_udf = F.udf(classify_signal_quality)

@F.udf(returnType=BooleanType())
def sla_breach_udf(slice_type, dl_tp, ul_tp, latency, pkt_loss, jitter):
    """
    Wrapper around qos_simulator.check_sla_breach for use in Spark pipelines.
    All numeric arguments arrive as Python floats or None.
    """
    return check_sla_breach(
        slice_type, dl_tp, ul_tp, latency, pkt_loss, jitter
    )

print("UDFs registered.")

# COMMAND ----------

# MAGIC %md ## 3. Bronze → Silver transform function

# COMMAND ----------

def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Apply type casting, enrichment, and validation to raw bronze events.

    Steps
    -----
    1. Cast all string fields to their proper types (drop malformed rows).
    2. Classify RSRP signal quality (Excellent / Good / Fair / Poor).
    3. Compute sla_breach flag using slice SLA thresholds.
    4. Stamp with ingested_at timestamp.

    Parameters
    ----------
    bronze_df : DataFrame
        Streaming or batch DataFrame with BRONZE_SCHEMA.

    Returns
    -------
    DataFrame
        Validated and enriched DataFrame with SILVER_SCHEMA.
    """
    silver_df = (
        bronze_df
        # ── Type casts ────────────────────────────────────────────────────
        .withColumn("event_timestamp",    F.to_timestamp("event_timestamp"))
        .withColumn("rsrp_dbm",           F.col("rsrp_dbm").cast(DoubleType()))
        .withColumn("rsrq_db",            F.col("rsrq_db").cast(DoubleType()))
        .withColumn("sinr_db",            F.col("sinr_db").cast(DoubleType()))
        .withColumn("cqi",                F.col("cqi").cast(IntegerType()))
        .withColumn("dl_throughput_mbps", F.col("dl_throughput_mbps").cast(DoubleType()))
        .withColumn("ul_throughput_mbps", F.col("ul_throughput_mbps").cast(DoubleType()))
        .withColumn("latency_ms",         F.col("latency_ms").cast(DoubleType()))
        .withColumn("jitter_ms",          F.col("jitter_ms").cast(DoubleType()))
        .withColumn("packet_loss_pct",    F.col("packet_loss_pct").cast(DoubleType()))
        .withColumn("mcs_index",          F.col("mcs_index").cast(IntegerType()))
        .withColumn("bler_pct",           F.col("bler_pct").cast(DoubleType()))
        .withColumn("rb_utilization_pct", F.col("rb_utilization_pct").cast(DoubleType()))
        .withColumn("latitude",           F.col("latitude").cast(DoubleType()))
        .withColumn("longitude",          F.col("longitude").cast(DoubleType()))
        .withColumn("handover_count",     F.col("handover_count").cast(IntegerType()))
        .withColumn("is_anomaly",         F.col("is_anomaly").cast(BooleanType()))
        # ── Drop rows with null primary key or timestamp ──────────────────
        .filter(
            F.col("event_id").isNotNull() &
            F.col("event_timestamp").isNotNull() &
            F.col("cell_id").isNotNull() &
            F.col("ue_id").isNotNull()
        )
        # ── Derived: signal quality classification ────────────────────────
        .withColumn(
            "signal_quality_class",
            classify_signal_quality_udf(F.col("rsrp_dbm"))
        )
        # ── Derived: SLA breach flag ──────────────────────────────────────
        .withColumn(
            "sla_breach",
            sla_breach_udf(
                F.col("slice_type"),
                F.col("dl_throughput_mbps"),
                F.col("ul_throughput_mbps"),
                F.col("latency_ms"),
                F.col("packet_loss_pct"),
                F.col("jitter_ms"),
            )
        )
        # ── Audit timestamp ───────────────────────────────────────────────
        .withColumn("ingested_at", F.current_timestamp())
        # ── Select only silver schema fields (drop bronze-only fields) ────
        .select(
            "event_id", "event_timestamp", "gnb_id", "cell_id", "ue_id",
            "slice_id", "slice_type",
            "rsrp_dbm", "rsrq_db", "sinr_db", "cqi",
            "dl_throughput_mbps", "ul_throughput_mbps",
            "latency_ms", "jitter_ms", "packet_loss_pct",
            "mcs_index", "bler_pct", "rb_utilization_pct",
            "latitude", "longitude", "handover_count", "is_anomaly",
            "signal_quality_class", "sla_breach", "ingested_at",
        )
    )
    return silver_df

print("Bronze → Silver transform defined.")

# COMMAND ----------

# MAGIC %md ## 4. Silver → Gold aggregation functions

# COMMAND ----------

def build_cell_kpi_agg(silver_df: DataFrame) -> DataFrame:
    """
    Compute sliding-window KPIs aggregated by cell_id.

    Uses a 1-minute sliding window with 30-second slide, watermarked at
    30 seconds to handle late-arriving events.

    Returns
    -------
    DataFrame
        Gold cell KPI schema.
    """
    return (
        silver_df
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            F.col("cell_id"),
            F.col("gnb_id"),
        )
        .agg(
            F.avg("rsrp_dbm").alias("avg_rsrp_dbm"),
            F.avg("rsrq_db").alias("avg_rsrq_db"),
            F.avg("sinr_db").alias("avg_sinr_db"),
            F.avg("cqi").alias("avg_cqi"),

            F.avg("dl_throughput_mbps").alias("avg_dl_throughput_mbps"),
            F.avg("ul_throughput_mbps").alias("avg_ul_throughput_mbps"),
            F.percentile_approx("dl_throughput_mbps", 0.95).alias("p95_dl_throughput_mbps"),

            F.avg("latency_ms").alias("avg_latency_ms"),
            F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
            F.avg("jitter_ms").alias("avg_jitter_ms"),

            F.avg("packet_loss_pct").alias("avg_packet_loss_pct"),
            F.avg("bler_pct").alias("avg_bler_pct"),
            F.avg("rb_utilization_pct").alias("avg_rb_utilization_pct"),

            F.count("*").alias("event_count"),
            F.countDistinct("ue_id").alias("unique_ues"),
            F.sum(F.col("sla_breach").cast("long")).alias("sla_breach_count"),
            F.sum(F.col("is_anomaly").cast("long")).alias("anomaly_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "cell_id", "gnb_id",
            "avg_rsrp_dbm", "avg_rsrq_db", "avg_sinr_db", "avg_cqi",
            "avg_dl_throughput_mbps", "avg_ul_throughput_mbps", "p95_dl_throughput_mbps",
            "avg_latency_ms", "p95_latency_ms", "avg_jitter_ms",
            "avg_packet_loss_pct", "avg_bler_pct", "avg_rb_utilization_pct",
            "event_count", "unique_ues", "sla_breach_count", "anomaly_count",
        )
    )


def build_slice_kpi_agg(silver_df: DataFrame) -> DataFrame:
    """
    Compute sliding-window KPIs aggregated by slice_id (network slice).

    Includes SLA compliance percentage for each window.
    """
    return (
        silver_df
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            F.col("slice_id"),
            F.col("slice_type"),
        )
        .agg(
            F.avg("dl_throughput_mbps").alias("avg_dl_throughput_mbps"),
            F.avg("ul_throughput_mbps").alias("avg_ul_throughput_mbps"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.percentile_approx("latency_ms", 0.99).alias("p99_latency_ms"),
            F.avg("packet_loss_pct").alias("avg_packet_loss_pct"),
            F.avg("jitter_ms").alias("avg_jitter_ms"),
            F.count("*").alias("event_count"),
            F.countDistinct("ue_id").alias("unique_ues"),
            F.sum(F.col("sla_breach").cast("long")).alias("sla_breach_count"),
        )
        .withColumn(
            "sla_compliance_pct",
            F.round(
                (1.0 - F.col("sla_breach_count") / F.col("event_count")) * 100.0,
                2
            )
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "slice_id", "slice_type",
            "avg_dl_throughput_mbps", "avg_ul_throughput_mbps",
            "avg_latency_ms", "p99_latency_ms",
            "avg_packet_loss_pct", "avg_jitter_ms",
            "event_count", "unique_ues", "sla_breach_count", "sla_compliance_pct",
        )
    )


def build_sla_violations(silver_df: DataFrame) -> DataFrame:
    """
    Explode SLA-breaching events into per-KPI violation rows.

    For each event that breaches the SLA, emit one row per violated KPI
    (e.g. an event could breach both latency and throughput simultaneously).

    Returns
    -------
    DataFrame
        Gold SLA violation schema.
    """
    # Load SLA thresholds from config
    sla_cfg = {s["type"]: s["sla"] for s in cfg["slices"]}

    # Build a set of per-KPI breach conditions
    # Each element: (violation_type, kpi_name, kpi_col, threshold_expr, severity)
    conditions = []
    for slice_type, sla in sla_cfg.items():
        conditions += [
            (
                "LOW_DL_THROUGHPUT", "dl_throughput_mbps",
                F.col("dl_throughput_mbps"),
                sla["min_dl_throughput_mbps"], "WARNING",
                slice_type
            ),
            (
                "LOW_UL_THROUGHPUT", "ul_throughput_mbps",
                F.col("ul_throughput_mbps"),
                sla["min_ul_throughput_mbps"], "WARNING",
                slice_type
            ),
            (
                "HIGH_LATENCY", "latency_ms",
                F.col("latency_ms"),
                sla["max_latency_ms"], "CRITICAL",
                slice_type
            ),
            (
                "HIGH_PACKET_LOSS", "packet_loss_pct",
                F.col("packet_loss_pct"),
                sla["max_packet_loss_pct"], "CRITICAL",
                slice_type
            ),
            (
                "HIGH_JITTER", "jitter_ms",
                F.col("jitter_ms"),
                sla["max_jitter_ms"], "WARNING",
                slice_type
            ),
        ]

    # Filter events that already triggered sla_breach flag, then union
    # one DataFrame per violation type.
    violation_dfs = []

    for vtype, kpi_name, kpi_col, threshold, severity, s_type in conditions:
        is_min = vtype.startswith("LOW_")
        breach_cond = (
            (kpi_col < threshold) if is_min else (kpi_col > threshold)
        )

        vdf = (
            silver_df
            .filter(
                (F.col("slice_type") == s_type) &
                F.col("sla_breach") &
                breach_cond &
                kpi_col.isNotNull()
            )
            .select(
                "event_id", "event_timestamp", "cell_id", "ue_id",
                "slice_id", "slice_type",
                F.lit(vtype).alias("violation_type"),
                F.lit(kpi_name).alias("kpi_name"),
                kpi_col.cast(DoubleType()).alias("kpi_value"),
                F.lit(float(threshold)).alias("threshold_value"),
                F.lit(severity).alias("severity"),
                F.current_timestamp().alias("detected_at"),
            )
        )
        violation_dfs.append(vdf)

    return violation_dfs[0].unionAll(*violation_dfs[1:]) if violation_dfs else silver_df.limit(0)


print("Gold aggregation functions defined.")

# COMMAND ----------

# MAGIC %md ## 5. Launch streaming queries
# MAGIC
# MAGIC Three concurrent streaming queries:
# MAGIC 1. **Bronze → Silver** (validation + enrichment)
# MAGIC 2. **Silver → Gold Cell KPIs** (windowed per-cell aggregations)
# MAGIC 3. **Silver → Gold Slice KPIs** (windowed per-slice aggregations)
# MAGIC 4. **Silver → Gold SLA Violations** (per-event breach detection)

# COMMAND ----------

# ── Query 1: Bronze → Silver ─────────────────────────────────────────────────

bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")   # CDF not required for append-only bronze
    .load(BRONZE_PATH)
)

silver_stream = transform_bronze_to_silver(bronze_stream)

q_bronze_to_silver = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT_BASE}/bronze_to_silver")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start(SILVER_PATH)
)

print(f"Query 1 started: {q_bronze_to_silver.id}  (Bronze → Silver)")

# COMMAND ----------

# ── Query 2: Silver → Gold Cell KPIs ─────────────────────────────────────────

silver_stream_for_cells = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

cell_kpi_stream = build_cell_kpi_agg(silver_stream_for_cells)

q_cell_kpis = (
    cell_kpi_stream.writeStream
    .format("delta")
    .outputMode("append")             # append is valid with watermark + window
    .option("checkpointLocation", f"{CKPT_BASE}/cell_kpis")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start(GOLD_CELL_PATH)
)

print(f"Query 2 started: {q_cell_kpis.id}  (Silver → Gold Cell KPIs)")

# COMMAND ----------

# ── Query 3: Silver → Gold Slice KPIs ────────────────────────────────────────

silver_stream_for_slices = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

slice_kpi_stream = build_slice_kpi_agg(silver_stream_for_slices)

q_slice_kpis = (
    slice_kpi_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT_BASE}/slice_kpis")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start(GOLD_SLICE_PATH)
)

print(f"Query 3 started: {q_slice_kpis.id}  (Silver → Gold Slice KPIs)")

# COMMAND ----------

# ── Query 4: Silver → Gold SLA Violations ────────────────────────────────────

silver_stream_for_sla = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

sla_violation_stream = build_sla_violations(silver_stream_for_sla)

q_sla_violations = (
    sla_violation_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT_BASE}/sla_violations")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start(GOLD_SLA_PATH)
)

print(f"Query 4 started: {q_sla_violations.id}  (Silver → Gold SLA Violations)")

# COMMAND ----------

# MAGIC %md ## 6. Monitor streaming query progress

# COMMAND ----------

import time

def print_streaming_status():
    """Print a summary of all active streaming query progress."""
    active = spark.streams.active
    if not active:
        print("No active streaming queries.")
        return

    print(f"\n{'Query':<40} {'Status':<15} {'Input rows/s':<15} {'Processed rows'}")
    print("-" * 90)
    for q in active:
        progress = q.lastProgress
        if progress:
            input_rate = progress.get("inputRowsPerSecond", 0) or 0
            processed  = progress.get("numInputRows", 0) or 0
            status     = q.status.get("message", "running")[:14]
        else:
            input_rate, processed, status = 0, 0, "starting…"
        print(f"{q.name or q.id[:38]:<40} {status:<15} {input_rate:<15.1f} {processed}")


# Poll for 60 seconds then leave running in the background
for i in range(6):
    print_streaming_status()
    time.sleep(10)

print("\nStreaming queries running in background — proceed to notebook 04.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stopping streaming queries
# MAGIC
# MAGIC To stop all streaming queries gracefully, run:
# MAGIC ```python
# MAGIC for q in spark.streams.active:
# MAGIC     q.stop()
# MAGIC ```
# MAGIC Or cancel this notebook cell.

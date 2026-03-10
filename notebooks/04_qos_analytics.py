# Databricks notebook source
# =============================================================================
# Notebook 04 — QoS Analytics & Reporting
# =============================================================================
# Purpose : Interactive analysis of the gold-layer QoS KPI tables produced by
#            the streaming pipeline.  Run this notebook after the streaming
#            pipeline (Notebook 03) has been running for at least one window
#            interval so that gold tables contain data.
#
# Sections
# ─────────
# 1.  Environment setup
# 2.  Network overview — event volumes and anomaly rate
# 3.  Signal quality analysis — RSRP / RSRQ / SINR distribution
# 4.  Throughput analysis — per-slice DL/UL throughput KPIs
# 5.  Latency analysis — per-slice latency percentiles
# 6.  Cell-level KPI heatmap — worst cells by RSRP and latency
# 7.  SLA compliance dashboard — per-slice compliance %
# 8.  SLA violation breakdown — violation type frequency
# 9.  Top-10 worst-performing cells
# 10. Anomaly trend — anomalies over time
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 5G QoS Analytics Dashboard
# MAGIC
# MAGIC This notebook provides interactive analysis of real-time 5G network
# MAGIC Quality-of-Service data, covering:
# MAGIC - **Signal quality** (RSRP, RSRQ, SINR)
# MAGIC - **Throughput** across eMBB, URLLC, and mMTC slices
# MAGIC - **Latency** percentiles (P50, P95, P99)
# MAGIC - **SLA compliance** per network slice
# MAGIC - **Anomaly detection** and top offending cells

# COMMAND ----------

# MAGIC %md ## 1. Environment setup

# COMMAND ----------

import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

REPO_ROOT = os.environ.get(
    "QOS_REPO_ROOT",
    "/Workspace/Repos/<your-org>/databricks-qos"
)
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

with open(os.path.join(REPO_ROOT, "config", "config.yaml")) as fh:
    cfg = yaml.safe_load(fh)

paths = cfg["paths"]

SILVER_PATH     = paths["silver"]
GOLD_CELL_PATH  = paths["gold_cell"]
GOLD_SLICE_PATH = paths["gold_slice"]
GOLD_SLA_PATH   = paths["gold_sla"]

# Load gold and silver tables as static DataFrames for interactive analysis
silver_df     = spark.read.format("delta").load(SILVER_PATH)
gold_cell_df  = spark.read.format("delta").load(GOLD_CELL_PATH)
gold_slice_df = spark.read.format("delta").load(GOLD_SLICE_PATH)
gold_sla_df   = spark.read.format("delta").load(GOLD_SLA_PATH)

# Register as temp views for SQL cells
silver_df.createOrReplaceTempView("silver_events")
gold_cell_df.createOrReplaceTempView("cell_kpis")
gold_slice_df.createOrReplaceTempView("slice_kpis")
gold_sla_df.createOrReplaceTempView("sla_violations")

print("Tables loaded and registered as temp views.")

# COMMAND ----------

# MAGIC %md ## 2. Network overview

# COMMAND ----------

# MAGIC %md ### 2.1 Event volume by slice type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     COUNT(*)                                        AS total_events,
# MAGIC     ROUND(AVG(CAST(is_anomaly AS INT)) * 100, 2)   AS anomaly_rate_pct,
# MAGIC     COUNT(DISTINCT cell_id)                         AS unique_cells,
# MAGIC     COUNT(DISTINCT ue_id)                           AS unique_ues,
# MAGIC     MIN(event_timestamp)                            AS earliest_event,
# MAGIC     MAX(event_timestamp)                            AS latest_event
# MAGIC FROM silver_events
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY total_events DESC

# COMMAND ----------

# MAGIC %md ### 2.2 Events per minute (event rate over time)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('minute', event_timestamp)  AS event_minute,
# MAGIC     slice_type,
# MAGIC     COUNT(*)                                AS events
# MAGIC FROM silver_events
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md ## 3. Signal quality analysis

# COMMAND ----------

# MAGIC %md ### 3.1 RSRP distribution by slice type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     signal_quality_class,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY slice_type), 2) AS pct_of_slice
# MAGIC FROM silver_events
# MAGIC WHERE rsrp_dbm IS NOT NULL
# MAGIC GROUP BY slice_type, signal_quality_class
# MAGIC ORDER BY slice_type,
# MAGIC     CASE signal_quality_class
# MAGIC         WHEN 'Excellent' THEN 1
# MAGIC         WHEN 'Good'      THEN 2
# MAGIC         WHEN 'Fair'      THEN 3
# MAGIC         WHEN 'Poor'      THEN 4
# MAGIC         ELSE 5
# MAGIC     END

# COMMAND ----------

# MAGIC %md ### 3.2 Signal KPI percentiles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(MIN(rsrp_dbm), 1)                          AS rsrp_min,
# MAGIC     ROUND(PERCENTILE(rsrp_dbm, 0.25), 1)             AS rsrp_p25,
# MAGIC     ROUND(PERCENTILE(rsrp_dbm, 0.50), 1)             AS rsrp_p50,
# MAGIC     ROUND(PERCENTILE(rsrp_dbm, 0.75), 1)             AS rsrp_p75,
# MAGIC     ROUND(MAX(rsrp_dbm), 1)                          AS rsrp_max,
# MAGIC     ROUND(PERCENTILE(sinr_db,  0.50), 1)             AS sinr_p50,
# MAGIC     ROUND(PERCENTILE(sinr_db,  0.05), 1)             AS sinr_p5_worst
# MAGIC FROM silver_events
# MAGIC WHERE rsrp_dbm IS NOT NULL AND sinr_db IS NOT NULL
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY slice_type

# COMMAND ----------

# MAGIC %md ## 4. Throughput analysis

# COMMAND ----------

# MAGIC %md ### 4.1 Downlink throughput KPIs per slice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(AVG(dl_throughput_mbps),   2)   AS avg_dl_mbps,
# MAGIC     ROUND(PERCENTILE(dl_throughput_mbps, 0.50), 2) AS p50_dl_mbps,
# MAGIC     ROUND(PERCENTILE(dl_throughput_mbps, 0.95), 2) AS p95_dl_mbps,
# MAGIC     ROUND(AVG(ul_throughput_mbps),   2)   AS avg_ul_mbps,
# MAGIC     ROUND(PERCENTILE(ul_throughput_mbps, 0.95), 2) AS p95_ul_mbps
# MAGIC FROM silver_events
# MAGIC WHERE dl_throughput_mbps IS NOT NULL
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY avg_dl_mbps DESC

# COMMAND ----------

# MAGIC %md ### 4.2 Windowed average throughput over time (from gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     window_start,
# MAGIC     slice_type,
# MAGIC     ROUND(avg_dl_throughput_mbps, 2) AS avg_dl_mbps,
# MAGIC     ROUND(avg_ul_throughput_mbps, 2) AS avg_ul_mbps,
# MAGIC     event_count
# MAGIC FROM slice_kpis
# MAGIC ORDER BY window_start, slice_type

# COMMAND ----------

# MAGIC %md ## 5. Latency analysis

# COMMAND ----------

# MAGIC %md ### 5.1 Latency percentiles per slice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.50), 3)  AS p50_latency_ms,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.95), 3)  AS p95_latency_ms,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.99), 3)  AS p99_latency_ms,
# MAGIC     ROUND(MAX(latency_ms),               3)  AS max_latency_ms,
# MAGIC     ROUND(AVG(jitter_ms),                3)  AS avg_jitter_ms,
# MAGIC     ROUND(AVG(packet_loss_pct),          5)  AS avg_packet_loss_pct
# MAGIC FROM silver_events
# MAGIC WHERE latency_ms IS NOT NULL
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY p50_latency_ms

# COMMAND ----------

# MAGIC %md ### 5.2 Latency trend over time (gold cell KPIs)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     window_start,
# MAGIC     cell_id,
# MAGIC     ROUND(avg_latency_ms, 2)  AS avg_latency_ms,
# MAGIC     ROUND(p95_latency_ms, 2)  AS p95_latency_ms,
# MAGIC     event_count
# MAGIC FROM cell_kpis
# MAGIC ORDER BY window_start, avg_latency_ms DESC
# MAGIC LIMIT 200

# COMMAND ----------

# MAGIC %md ## 6. Cell-level KPI summary

# COMMAND ----------

# MAGIC %md ### 6.1 Average KPIs per cell (latest window)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest completed window per cell
# MAGIC WITH latest_window AS (
# MAGIC     SELECT cell_id, MAX(window_end) AS latest_end
# MAGIC     FROM cell_kpis
# MAGIC     GROUP BY cell_id
# MAGIC ),
# MAGIC latest_kpis AS (
# MAGIC     SELECT c.*
# MAGIC     FROM cell_kpis c
# MAGIC     JOIN latest_window lw
# MAGIC       ON c.cell_id = lw.cell_id AND c.window_end = lw.latest_end
# MAGIC )
# MAGIC SELECT
# MAGIC     cell_id,
# MAGIC     gnb_id,
# MAGIC     ROUND(avg_rsrp_dbm, 1)           AS rsrp_dbm,
# MAGIC     ROUND(avg_sinr_db, 1)            AS sinr_db,
# MAGIC     ROUND(avg_dl_throughput_mbps, 1) AS dl_mbps,
# MAGIC     ROUND(avg_latency_ms, 2)         AS latency_ms,
# MAGIC     ROUND(avg_rb_utilization_pct, 1) AS rb_util_pct,
# MAGIC     event_count,
# MAGIC     unique_ues,
# MAGIC     sla_breach_count,
# MAGIC     anomaly_count
# MAGIC FROM latest_kpis
# MAGIC ORDER BY sla_breach_count DESC

# COMMAND ----------

# MAGIC %md ## 7. SLA compliance dashboard

# COMMAND ----------

# MAGIC %md ### 7.1 Overall SLA compliance per slice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     COUNT(*)                                              AS total_events,
# MAGIC     SUM(CAST(sla_breach AS INT))                         AS sla_breaches,
# MAGIC     ROUND(
# MAGIC         (1 - SUM(CAST(sla_breach AS INT)) / COUNT(*)) * 100,
# MAGIC         2
# MAGIC     )                                                    AS compliance_pct
# MAGIC FROM silver_events
# MAGIC WHERE slice_type IS NOT NULL
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY compliance_pct

# COMMAND ----------

# MAGIC %md ### 7.2 SLA compliance trend from gold slice KPIs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     window_start,
# MAGIC     slice_type,
# MAGIC     sla_compliance_pct,
# MAGIC     sla_breach_count,
# MAGIC     event_count
# MAGIC FROM slice_kpis
# MAGIC ORDER BY window_start, slice_type

# COMMAND ----------

# MAGIC %md ## 8. SLA violation breakdown

# COMMAND ----------

# MAGIC %md ### 8.1 Violation types by frequency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     violation_type,
# MAGIC     severity,
# MAGIC     slice_type,
# MAGIC     COUNT(*)                     AS violation_count,
# MAGIC     COUNT(DISTINCT cell_id)      AS affected_cells,
# MAGIC     COUNT(DISTINCT ue_id)        AS affected_ues,
# MAGIC     ROUND(AVG(kpi_value), 3)     AS avg_kpi_value,
# MAGIC     ROUND(AVG(threshold_value),3) AS threshold
# MAGIC FROM sla_violations
# MAGIC GROUP BY violation_type, severity, slice_type
# MAGIC ORDER BY violation_count DESC

# COMMAND ----------

# MAGIC %md ### 8.2 SLA violations over time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('minute', event_timestamp) AS minute,
# MAGIC     violation_type,
# MAGIC     severity,
# MAGIC     COUNT(*) AS violation_count
# MAGIC FROM sla_violations
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, violation_count DESC

# COMMAND ----------

# MAGIC %md ## 9. Top-10 worst-performing cells

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cells with the most SLA breaches and poorest average RSRP
# MAGIC WITH cell_summary AS (
# MAGIC     SELECT
# MAGIC         cell_id,
# MAGIC         gnb_id,
# MAGIC         COUNT(*)                                      AS total_events,
# MAGIC         ROUND(AVG(rsrp_dbm), 1)                      AS avg_rsrp_dbm,
# MAGIC         ROUND(AVG(sinr_db), 1)                       AS avg_sinr_db,
# MAGIC         ROUND(AVG(latency_ms), 2)                    AS avg_latency_ms,
# MAGIC         ROUND(AVG(rb_utilization_pct), 1)            AS avg_rb_util_pct,
# MAGIC         SUM(CAST(sla_breach AS INT))                 AS sla_breaches,
# MAGIC         SUM(CAST(is_anomaly AS INT))                 AS anomaly_count,
# MAGIC         ROUND(
# MAGIC             SUM(CAST(sla_breach AS INT)) * 100.0 / COUNT(*),
# MAGIC             2
# MAGIC         )                                            AS breach_rate_pct
# MAGIC     FROM silver_events
# MAGIC     GROUP BY cell_id, gnb_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM cell_summary
# MAGIC ORDER BY sla_breaches DESC, avg_rsrp_dbm ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md ## 10. Anomaly trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('minute', event_timestamp) AS minute,
# MAGIC     slice_type,
# MAGIC     COUNT(*)                               AS total_events,
# MAGIC     SUM(CAST(is_anomaly AS INT))           AS anomaly_count,
# MAGIC     ROUND(SUM(CAST(is_anomaly AS INT)) * 100.0 / COUNT(*), 2) AS anomaly_rate_pct
# MAGIC FROM silver_events
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md ### 10.1 Anomaly breakdown by type (from silver)

# COMMAND ----------

# Approximate anomaly kind breakdown using a proxy heuristic
# (In production, anomaly_kind would be stored in bronze/silver.)
from pyspark.sql.functions import (
    col, when, count, round as spark_round
)

anomaly_proxy = (
    silver_df.filter(col("is_anomaly"))
    .withColumn(
        "anomaly_proxy_type",
        when((col("sinr_db") < 0) & (col("rsrp_dbm") < -110), "Interference")
        .when(col("latency_ms") > 100, "Handover / Backhaul")
        .when(col("rb_utilization_pct") > 85, "Cell Congestion")
        .otherwise("Other")
    )
    .groupBy("anomaly_proxy_type", "slice_type")
    .agg(count("*").alias("count"))
    .orderBy("count", ascending=False)
)

display(anomaly_proxy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The analysis above demonstrates end-to-end 5G QoS observability on Databricks:
# MAGIC
# MAGIC | Capability | Implementation |
# MAGIC |-----------|----------------|
# MAGIC | Real-time ingestion | Delta Live Tables / Structured Streaming |
# MAGIC | Signal quality monitoring | RSRP/RSRQ/SINR classification |
# MAGIC | Throughput tracking | DL/UL KPIs per slice and cell |
# MAGIC | Latency SLAs | P50/P95/P99 per URLLC / eMBB / mMTC |
# MAGIC | SLA compliance | Per-slice breach rate and compliance % |
# MAGIC | Anomaly detection | Heuristic + flag propagation |
# MAGIC | Cell ranking | Top offenders by breach count |

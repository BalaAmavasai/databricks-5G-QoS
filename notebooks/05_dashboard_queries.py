# Databricks notebook source
# =============================================================================
# Notebook 05 — Dashboard SQL Queries
# =============================================================================
# Purpose : Self-contained SQL queries designed to be copy-pasted into
#            Databricks SQL dashboards or used as saved queries.
#
# Each query is labelled with the recommended visualisation type.
# Update table references if using Unity Catalog:
#   FROM main.qos_5g_demo.cell_kpis
# instead of temp view names.
#
# Prerequisites: Notebook 03 (streaming pipeline) has populated the gold tables.
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 5G QoS Dashboard — SQL Query Library
# MAGIC
# MAGIC Copy these queries into **Databricks SQL** → **Queries** to build a
# MAGIC real-time operations dashboard.
# MAGIC
# MAGIC | # | Query | Visualisation |
# MAGIC |---|-------|---------------|
# MAGIC | Q1 | Network heartbeat (event rate) | Counter / Time series |
# MAGIC | Q2 | SLA compliance scorecard | Counter |
# MAGIC | Q3 | Throughput by slice | Bar chart |
# MAGIC | Q4 | Latency percentiles | Bar chart |
# MAGIC | Q5 | Cell signal quality heatmap | Table / Heat |
# MAGIC | Q6 | Top SLA violations | Table |
# MAGIC | Q7 | Anomaly rate trend | Line chart |
# MAGIC | Q8 | Resource utilisation | Gauge / Bar |
# MAGIC | Q9 | Per-cell breach leaderboard | Table |
# MAGIC | Q10 | URLLC latency gauge | Gauge |

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q1 — Network Heartbeat
# MAGIC **Visualisation**: Counter (Total Events) + Time-series (Events/min)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1a: Total event count (Counter widget)
# MAGIC SELECT COUNT(*) AS total_events FROM silver_events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1b: Events per minute (Time-series line chart)
# MAGIC SELECT
# MAGIC     DATE_TRUNC('minute', event_timestamp) AS ts,
# MAGIC     COUNT(*)                               AS events_per_minute
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 1 HOUR
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q2 — SLA Compliance Scorecard
# MAGIC **Visualisation**: 3 × Counter widgets (one per slice)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: SLA compliance % per slice (use as 3 separate Counter widgets)
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(
# MAGIC         (1 - SUM(CAST(sla_breach AS INT)) / NULLIF(COUNT(*), 0)) * 100,
# MAGIC         2
# MAGIC     ) AS sla_compliance_pct
# MAGIC FROM silver_events
# MAGIC WHERE slice_type IS NOT NULL
# MAGIC   AND event_timestamp >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY slice_type

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q3 — Throughput by Slice
# MAGIC **Visualisation**: Grouped bar chart (DL vs UL, grouped by slice)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(AVG(dl_throughput_mbps), 2)               AS avg_dl_mbps,
# MAGIC     ROUND(PERCENTILE(dl_throughput_mbps, 0.95), 2)  AS p95_dl_mbps,
# MAGIC     ROUND(AVG(ul_throughput_mbps), 2)               AS avg_ul_mbps,
# MAGIC     ROUND(PERCENTILE(ul_throughput_mbps, 0.95), 2)  AS p95_ul_mbps
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY avg_dl_mbps DESC

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q4 — Latency Percentiles
# MAGIC **Visualisation**: Bar chart with P50, P95, P99 per slice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     slice_type,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.50), 3) AS p50_ms,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.95), 3) AS p95_ms,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.99), 3) AS p99_ms,
# MAGIC     ROUND(MAX(latency_ms),               3) AS max_ms
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY slice_type
# MAGIC ORDER BY p50_ms

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q5 — Cell Signal Quality Summary
# MAGIC **Visualisation**: Table with conditional formatting on RSRP colour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cell_id,
# MAGIC     gnb_id,
# MAGIC     ROUND(AVG(rsrp_dbm),           1)  AS avg_rsrp_dbm,
# MAGIC     ROUND(AVG(sinr_db),            1)  AS avg_sinr_db,
# MAGIC     ROUND(AVG(cqi),                2)  AS avg_cqi,
# MAGIC     -- Map signal class to a numeric score for heat colouring
# MAGIC     ROUND(AVG(
# MAGIC         CASE signal_quality_class
# MAGIC             WHEN 'Excellent' THEN 4
# MAGIC             WHEN 'Good'      THEN 3
# MAGIC             WHEN 'Fair'      THEN 2
# MAGIC             WHEN 'Poor'      THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS signal_score,
# MAGIC     COUNT(*) AS event_count
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY cell_id, gnb_id
# MAGIC ORDER BY avg_rsrp_dbm ASC

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q6 — Top SLA Violations
# MAGIC **Visualisation**: Table (sort by violation_count DESC)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     violation_type,
# MAGIC     severity,
# MAGIC     slice_type,
# MAGIC     COUNT(*)                     AS violation_count,
# MAGIC     COUNT(DISTINCT cell_id)      AS cells_affected,
# MAGIC     COUNT(DISTINCT ue_id)        AS ues_affected,
# MAGIC     ROUND(AVG(kpi_value),   4)   AS avg_kpi_value,
# MAGIC     ROUND(MAX(kpi_value),   4)   AS max_kpi_value,
# MAGIC     ROUND(AVG(threshold_value), 4) AS sla_threshold
# MAGIC FROM sla_violations
# MAGIC WHERE detected_at >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY violation_type, severity, slice_type
# MAGIC ORDER BY violation_count DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q7 — Anomaly Rate Trend
# MAGIC **Visualisation**: Area / Line chart, x=ts, y=anomaly_rate_pct

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('minute', event_timestamp)          AS ts,
# MAGIC     ROUND(AVG(CAST(is_anomaly AS INT)) * 100, 2)  AS anomaly_rate_pct,
# MAGIC     COUNT(*)                                        AS total_events,
# MAGIC     SUM(CAST(is_anomaly AS INT))                   AS anomaly_count
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 1 HOUR
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q8 — Resource Block Utilisation
# MAGIC **Visualisation**: Gauge or bar chart per cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cell_id,
# MAGIC     ROUND(AVG(rb_utilization_pct), 1) AS avg_rb_util_pct,
# MAGIC     CASE
# MAGIC         WHEN AVG(rb_utilization_pct) >= 85 THEN 'Congested'
# MAGIC         WHEN AVG(rb_utilization_pct) >= 60 THEN 'High Load'
# MAGIC         WHEN AVG(rb_utilization_pct) >= 30 THEN 'Normal'
# MAGIC         ELSE 'Low Load'
# MAGIC     END AS load_status
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 15 MINUTE
# MAGIC GROUP BY cell_id
# MAGIC ORDER BY avg_rb_util_pct DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q9 — Cell Breach Leaderboard
# MAGIC **Visualisation**: Bar chart, sorted by breach_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cell_id,
# MAGIC     gnb_id,
# MAGIC     SUM(CAST(sla_breach AS INT))                              AS breach_count,
# MAGIC     COUNT(*)                                                   AS total_events,
# MAGIC     ROUND(SUM(CAST(sla_breach AS INT)) * 100.0 / COUNT(*), 1) AS breach_rate_pct,
# MAGIC     ROUND(AVG(rsrp_dbm), 1)                                   AS avg_rsrp_dbm,
# MAGIC     ROUND(AVG(latency_ms), 2)                                 AS avg_latency_ms
# MAGIC FROM silver_events
# MAGIC WHERE event_timestamp >= NOW() - INTERVAL 30 MINUTE
# MAGIC GROUP BY cell_id, gnb_id
# MAGIC ORDER BY breach_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Q10 — URLLC Latency Gauge
# MAGIC **Visualisation**: Gauge widget (target < 1 ms)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Real-time URLLC latency — suitable for a Gauge widget with target=1ms
# MAGIC SELECT
# MAGIC     'URLLC'                                                AS slice,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.99), 3)                AS p99_latency_ms,
# MAGIC     ROUND(PERCENTILE(latency_ms, 0.95), 3)                AS p95_latency_ms,
# MAGIC     ROUND(AVG(latency_ms), 3)                             AS avg_latency_ms,
# MAGIC     1.0                                                    AS sla_target_ms,
# MAGIC     ROUND(
# MAGIC         SUM(CAST(latency_ms > 1.0 AS INT)) * 100.0 / COUNT(*),
# MAGIC         2
# MAGIC     )                                                      AS pct_exceeding_sla
# MAGIC FROM silver_events
# MAGIC WHERE slice_type = 'URLLC'
# MAGIC   AND event_timestamp >= NOW() - INTERVAL 5 MINUTE
# MAGIC   AND latency_ms IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Building a Databricks SQL Dashboard
# MAGIC
# MAGIC 1. Go to **Databricks SQL → Dashboards → New Dashboard**
# MAGIC 2. Click **Add Visualisation** and select each query above
# MAGIC 3. Suggested layout:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │  [Q2: eMBB SLA %]  [Q2: URLLC SLA %]  [Q2: mMTC SLA %]  [Q1: Events] │
# MAGIC ├─────────────────────────────────────────────────────────────────────┤
# MAGIC │  [Q3: Throughput bar chart]    │  [Q4: Latency percentiles]          │
# MAGIC ├─────────────────────────────────────────────────────────────────────┤
# MAGIC │  [Q7: Anomaly rate time series (full width)]                         │
# MAGIC ├─────────────────────────────────────────────────────────────────────┤
# MAGIC │  [Q9: Cell breach leaderboard] │  [Q6: SLA violation table]          │
# MAGIC ├─────────────────────────────────────────────────────────────────────┤
# MAGIC │  [Q10: URLLC latency gauge]    │  [Q8: RB utilisation bar]           │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC 4. Set dashboard **Auto-refresh** to 30 seconds for near-real-time updates.

"""
schema.py — Spark schema definitions for the 5G QoS streaming pipeline.

All layers (bronze, silver, gold) share a common vocabulary of field names
so that transformations between layers are transparent and traceable.

Layers
------
- Bronze  : Raw UE event exactly as produced by the simulator / ingested
            from a real source (Kafka, Event Hubs, etc.).
- Silver  : Validated, enriched, and type-cast version of bronze events.
- Gold    : Aggregated KPI tables (per-cell, per-slice, SLA violations).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
)

# ---------------------------------------------------------------------------
# Bronze schema
# Fields arrive as strings from JSON payloads; numeric types are strings
# so the silver layer can catch parse errors cleanly.
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = StructType([
    # --- Event metadata ---
    StructField("event_id",           StringType(),    nullable=False),
    StructField("event_timestamp",    StringType(),    nullable=False),  # ISO-8601 string

    # --- Network topology identifiers ---
    StructField("gnb_id",             StringType(),    nullable=False),  # gNodeB (5G base station)
    StructField("cell_id",            StringType(),    nullable=False),  # Serving cell
    StructField("ue_id",              StringType(),    nullable=False),  # User Equipment
    StructField("slice_id",           StringType(),    nullable=True),   # Network slice (S1/S2/S3)
    StructField("slice_type",         StringType(),    nullable=True),   # eMBB / URLLC / mMTC

    # --- Signal quality (as strings to survive bad payloads) ---
    StructField("rsrp_dbm",           StringType(),    nullable=True),   # -140 .. -44 dBm
    StructField("rsrq_db",            StringType(),    nullable=True),   # -19.5 .. -3 dB
    StructField("sinr_db",            StringType(),    nullable=True),   # -23 .. +40 dB
    StructField("cqi",                StringType(),    nullable=True),   # 0..15

    # --- Throughput ---
    StructField("dl_throughput_mbps", StringType(),    nullable=True),
    StructField("ul_throughput_mbps", StringType(),    nullable=True),

    # --- Latency / reliability ---
    StructField("latency_ms",         StringType(),    nullable=True),
    StructField("jitter_ms",          StringType(),    nullable=True),
    StructField("packet_loss_pct",    StringType(),    nullable=True),

    # --- Radio resource metrics ---
    StructField("mcs_index",          StringType(),    nullable=True),   # 0..28
    StructField("bler_pct",           StringType(),    nullable=True),   # Block Error Rate
    StructField("rb_utilization_pct", StringType(),    nullable=True),   # Resource Block %

    # --- UE location ---
    StructField("latitude",           StringType(),    nullable=True),
    StructField("longitude",          StringType(),    nullable=True),

    # --- Session metadata ---
    StructField("handover_count",     StringType(),    nullable=True),
    StructField("is_anomaly",         StringType(),    nullable=True),   # "true"/"false"
])

# ---------------------------------------------------------------------------
# Silver schema
# All numeric fields are properly typed; timestamps are TimestampType.
# ---------------------------------------------------------------------------
SILVER_SCHEMA = StructType([
    # --- Event metadata ---
    StructField("event_id",           StringType(),    nullable=False),
    StructField("event_timestamp",    TimestampType(), nullable=False),

    # --- Network topology ---
    StructField("gnb_id",             StringType(),    nullable=False),
    StructField("cell_id",            StringType(),    nullable=False),
    StructField("ue_id",              StringType(),    nullable=False),
    StructField("slice_id",           StringType(),    nullable=True),
    StructField("slice_type",         StringType(),    nullable=True),

    # --- Signal quality ---
    StructField("rsrp_dbm",           DoubleType(),    nullable=True),
    StructField("rsrq_db",            DoubleType(),    nullable=True),
    StructField("sinr_db",            DoubleType(),    nullable=True),
    StructField("cqi",                IntegerType(),   nullable=True),

    # --- Throughput ---
    StructField("dl_throughput_mbps", DoubleType(),    nullable=True),
    StructField("ul_throughput_mbps", DoubleType(),    nullable=True),

    # --- Latency / reliability ---
    StructField("latency_ms",         DoubleType(),    nullable=True),
    StructField("jitter_ms",          DoubleType(),    nullable=True),
    StructField("packet_loss_pct",    DoubleType(),    nullable=True),

    # --- Radio resource metrics ---
    StructField("mcs_index",          IntegerType(),   nullable=True),
    StructField("bler_pct",           DoubleType(),    nullable=True),
    StructField("rb_utilization_pct", DoubleType(),    nullable=True),

    # --- UE location ---
    StructField("latitude",           DoubleType(),    nullable=True),
    StructField("longitude",          DoubleType(),    nullable=True),

    # --- Session metadata ---
    StructField("handover_count",     IntegerType(),   nullable=True),
    StructField("is_anomaly",         BooleanType(),   nullable=True),

    # --- Silver-layer derived fields ---
    StructField("signal_quality_class", StringType(), nullable=True),  # Excellent/Good/Fair/Poor
    StructField("sla_breach",           BooleanType(),nullable=True),  # Does event breach slice SLA?
    StructField("ingested_at",          TimestampType(),nullable=False),
])

# ---------------------------------------------------------------------------
# Gold — per-cell windowed KPIs
# ---------------------------------------------------------------------------
GOLD_CELL_KPI_SCHEMA = StructType([
    StructField("window_start",         TimestampType(), nullable=False),
    StructField("window_end",           TimestampType(), nullable=False),
    StructField("cell_id",              StringType(),    nullable=False),
    StructField("gnb_id",              StringType(),    nullable=False),

    # Aggregated signal quality
    StructField("avg_rsrp_dbm",         DoubleType(),    nullable=True),
    StructField("avg_rsrq_db",          DoubleType(),    nullable=True),
    StructField("avg_sinr_db",          DoubleType(),    nullable=True),
    StructField("avg_cqi",              DoubleType(),    nullable=True),

    # Aggregated throughput
    StructField("avg_dl_throughput_mbps", DoubleType(), nullable=True),
    StructField("avg_ul_throughput_mbps", DoubleType(), nullable=True),
    StructField("p95_dl_throughput_mbps", DoubleType(), nullable=True),

    # Aggregated latency
    StructField("avg_latency_ms",       DoubleType(),    nullable=True),
    StructField("p95_latency_ms",       DoubleType(),    nullable=True),
    StructField("avg_jitter_ms",        DoubleType(),    nullable=True),

    # Reliability
    StructField("avg_packet_loss_pct",  DoubleType(),    nullable=True),
    StructField("avg_bler_pct",         DoubleType(),    nullable=True),

    # Resource utilisation
    StructField("avg_rb_utilization_pct", DoubleType(), nullable=True),

    # Event counts
    StructField("event_count",          LongType(),      nullable=False),
    StructField("unique_ues",           LongType(),      nullable=False),
    StructField("sla_breach_count",     LongType(),      nullable=False),
    StructField("anomaly_count",        LongType(),      nullable=False),
])

# ---------------------------------------------------------------------------
# Gold — per-slice windowed KPIs
# ---------------------------------------------------------------------------
GOLD_SLICE_KPI_SCHEMA = StructType([
    StructField("window_start",         TimestampType(), nullable=False),
    StructField("window_end",           TimestampType(), nullable=False),
    StructField("slice_id",             StringType(),    nullable=False),
    StructField("slice_type",           StringType(),    nullable=True),

    StructField("avg_dl_throughput_mbps", DoubleType(), nullable=True),
    StructField("avg_ul_throughput_mbps", DoubleType(), nullable=True),
    StructField("avg_latency_ms",       DoubleType(),    nullable=True),
    StructField("p99_latency_ms",       DoubleType(),    nullable=True),
    StructField("avg_packet_loss_pct",  DoubleType(),    nullable=True),
    StructField("avg_jitter_ms",        DoubleType(),    nullable=True),

    StructField("event_count",          LongType(),      nullable=False),
    StructField("unique_ues",           LongType(),      nullable=False),
    StructField("sla_breach_count",     LongType(),      nullable=False),
    StructField("sla_compliance_pct",   DoubleType(),    nullable=True),
])

# ---------------------------------------------------------------------------
# Gold — SLA violation events (one row per event that breaches a threshold)
# ---------------------------------------------------------------------------
GOLD_SLA_VIOLATION_SCHEMA = StructType([
    StructField("event_id",           StringType(),    nullable=False),
    StructField("event_timestamp",    TimestampType(), nullable=False),
    StructField("cell_id",            StringType(),    nullable=False),
    StructField("ue_id",              StringType(),    nullable=False),
    StructField("slice_id",           StringType(),    nullable=True),
    StructField("slice_type",         StringType(),    nullable=True),

    # Which KPI breached?
    StructField("violation_type",     StringType(),    nullable=False),  # e.g. "HIGH_LATENCY"
    StructField("kpi_name",           StringType(),    nullable=False),  # e.g. "latency_ms"
    StructField("kpi_value",          DoubleType(),    nullable=False),
    StructField("threshold_value",    DoubleType(),    nullable=False),
    StructField("severity",           StringType(),    nullable=False),  # CRITICAL / WARNING

    StructField("detected_at",        TimestampType(), nullable=False),
])

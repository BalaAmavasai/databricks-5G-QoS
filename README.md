# Databricks 5G QoS Streaming Demo

Created by Bala Amavasai <bala.amavasai@gmail.com> and Claude Code

A fully self-contained demonstration of **real-time Quality-of-Service (QoS)
analytics** for 5G mobile networks running on **Databricks Structured Streaming**
and **Delta Lake**.

---

## Overview

Mobile network operators generate continuous telemetry from thousands of
5G base stations (gNodeBs) and User Equipment (UEs). This demo simulates that
data stream and shows how Databricks can ingest, validate, aggregate, and
visualise QoS KPIs in near-real-time — powering NOC dashboards, SLA monitoring,
and automated alerting.

### What is simulated

| Parameter | Description | Range |
|-----------|-------------|-------|
| **RSRP** | Reference Signal Received Power | −140 .. −44 dBm |
| **RSRQ** | Reference Signal Received Quality | −19.5 .. −3 dB |
| **SINR** | Signal-to-Interference-plus-Noise Ratio | −23 .. +40 dB |
| **CQI** | Channel Quality Indicator | 0 .. 15 |
| **DL Throughput** | Downlink data rate | slice-dependent |
| **UL Throughput** | Uplink data rate | slice-dependent |
| **Latency** | Round-trip time | slice-dependent |
| **Jitter** | Latency variation | slice-dependent |
| **Packet Loss** | % of lost packets | 0 .. 10% |
| **MCS Index** | Modulation & Coding Scheme | 0 .. 28 |
| **BLER** | Block Error Rate | 0 .. 50% |
| **RB Utilisation** | Resource Block occupancy | 0 .. 100% |

### 5G Network Slices

| Slice | Type | Key SLAs |
|-------|------|----------|
| **S1** | eMBB — Enhanced Mobile Broadband | DL ≥ 100 Mbps, latency ≤ 10 ms |
| **S2** | URLLC — Ultra-Reliable Low Latency | Latency ≤ 1 ms, packet loss ≤ 0.001% |
| **S3** | mMTC — Massive Machine Type Comms | Latency ≤ 100 ms (IoT) |

### Anomaly injection

A configurable fraction (default 5%) of events simulate degraded radio conditions:

- **Interference spike** — RSRP/SINR drop, high BLER
- **Handover failure** — latency spike, packet loss surge
- **Cell congestion** — RB utilisation saturation, throughput collapse
- **Backhaul degradation** — high latency and jitter

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Notebook 02: Data Simulator                                        │
│  qos_simulator.generate_batch() → Spark DataFrame                   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ append
                            ▼
                   ┌─────────────────┐
                   │  Bronze Delta   │  Raw UE events (string types)
                   │  Table          │
                   └────────┬────────┘
                            │ readStream
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Notebook 03: Streaming Pipeline                                    │
│  • Type casting & validation                                        │
│  • Signal quality classification (RSRP → Excellent/Good/Fair/Poor)  │
│  • SLA breach detection (per slice SLA thresholds)                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ append
                            ▼
                   ┌─────────────────┐
                   │  Silver Delta   │  Clean, typed, enriched events
                   │  Table          │
                   └────────┬────────┘
                            │ readStream (3 concurrent queries)
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
    ┌──────────────┐ ┌────────────┐ ┌───────────────┐
    │ Gold: Cell   │ │Gold: Slice │ │ Gold: SLA     │
    │ KPIs         │ │KPIs        │ │ Violations    │
    │ (windowed)   │ │(windowed)  │ │ (per-event)   │
    └──────────────┘ └────────────┘ └───────────────┘
             │              │              │
             └──────────────┴──────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Notebook 04: QoS Analytics  |  Notebook 05: Dashboard SQL Queries  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Repository structure

```
databricks-qos/
├── README.md                       # This file
├── .gitignore
│
├── config/
│   └── config.yaml                 # Paths, SLA thresholds, simulator settings
│
├── src/
│   ├── __init__.py
│   ├── schema.py                   # Spark schema definitions (bronze/silver/gold)
│   └── qos_simulator.py            # 5G QoS event generator (pure Python)
│
├── notebooks/
│   ├── 01_setup.py                 # Create Delta tables, verify environment
│   ├── 02_data_simulator.py        # Generate & stream events to bronze table
│   ├── 03_streaming_pipeline.py    # Structured Streaming bronze→silver→gold
│   ├── 04_qos_analytics.py         # Interactive KPI analysis (SQL + PySpark)
│   └── 05_dashboard_queries.py     # SQL queries for Databricks SQL dashboards
│
└── tests/
    ├── __init__.py
    └── test_simulator.py           # pytest unit tests (no Spark required)
```

---

## Getting started

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Databricks Runtime | 13.x or later |
| Delta Lake | Bundled with DBR 13+ |
| Python | 3.9+ |
| PyYAML | ≥ 6.0 (installed by notebook 01) |

### Step 1 — Clone the repository into Databricks Repos

1. In Databricks, go to **Repos → Add Repo**
2. Enter this GitHub URL: `https://github.com/BalaAmavasai/databricks-5G-QoS`
3. Click **Create Repo**

### Step 2 — Set the repo root path

In each notebook, update `REPO_ROOT` to match your Repos path:

```python
REPO_ROOT = "/Workspace/Repos/<your-username>/databricks-5G-QoS"
```

Or set it as a cluster environment variable:
```
QOS_REPO_ROOT = /Workspace/Repos/<your-username>/databricks-5G-QoS
```

### Step 3 — Configure Delta Lake paths (optional)

Edit `config/config.yaml` to change storage paths or Unity Catalog settings.
The defaults write to `/tmp/qos_5g_demo/` on DBFS.

### Step 4 — Run notebook 01 (setup)

Run **`notebooks/01_setup.py`** once to:
- Install PyYAML
- Create all Delta tables
- Enable Change Data Feed

### Step 5 — Start the data simulator

Run **`notebooks/02_data_simulator.py`** and leave it running. It will
continuously write batches of simulated 5G events to the bronze Delta table.

Use the Databricks widgets to tune:
- `num_batches` — set to `0` for infinite streaming
- `events_per_batch` — default: 200 events/batch
- `anomaly_rate` — default: 0.05 (5%)
- `batch_interval` — default: 5 seconds

### Step 6 — Start the streaming pipeline

Run **`notebooks/03_streaming_pipeline.py`**.  This starts four concurrent
Structured Streaming queries that process the bronze events and populate the
silver and gold Delta tables.

The notebook monitors query progress for 60 seconds then leaves the queries
running in the background.

### Step 7 — Explore analytics

Run **`notebooks/04_qos_analytics.py`** for interactive SQL and PySpark
analysis, including:
- Signal quality distribution
- Throughput and latency KPIs
- SLA compliance per slice
- Cell breach leaderboard
- Anomaly trend over time

### Step 8 — Build a dashboard (optional)

Copy queries from **`notebooks/05_dashboard_queries.py`** into
**Databricks SQL → Queries** to build a real-time operations dashboard.
Set auto-refresh to 30 seconds.

---

## Running tests locally

The `src/qos_simulator.py` module has no Spark dependency and can be tested
on any Python 3.9+ environment:

```bash
pip install pytest
pytest tests/ -v
```

Expected output:
```
tests/test_simulator.py::TestGenerateEvent::test_all_schema_fields_present PASSED
tests/test_simulator.py::TestGenerateEvent::test_all_values_are_strings     PASSED
...
45 passed in 0.35s
```

---

## Configuration reference

### `config/config.yaml`

| Section | Key | Description |
|---------|-----|-------------|
| `paths` | `bronze` | DBFS path for raw bronze events |
| `paths` | `silver` | DBFS path for clean silver events |
| `paths` | `gold_cell` | DBFS path for per-cell KPI aggregations |
| `paths` | `gold_slice` | DBFS path for per-slice KPI aggregations |
| `paths` | `gold_sla` | DBFS path for SLA violation events |
| `simulator` | `num_cells` | Number of simulated gNodeB cells (default: 20) |
| `simulator` | `events_per_second` | Target event rate |
| `simulator` | `anomaly_rate` | Fraction of degraded events (0–1) |
| `streaming` | `window_duration` | KPI aggregation window size |
| `streaming` | `slide_duration` | Sliding window step |
| `streaming` | `watermark_delay` | Late-data tolerance |
| `slices` | `sla` | Per-slice SLA thresholds |
| `alerts` | `rsrp_poor_threshold_dbm` | Signal quality alert threshold |

---

## Adapting to a real data source

To connect to a real 5G data feed, replace the `readStream` in
`notebook 03` with your actual source:

**Kafka / Confluent:**
```python
bronze_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "<broker>:9092")
    .option("subscribe", "5g-qos-events")
    .load()
    .selectExpr("CAST(value AS STRING) AS json_payload")
    .select(F.from_json("json_payload", BRONZE_SCHEMA).alias("d"))
    .select("d.*")
)
```

**Azure Event Hubs:**
```python
bronze_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**event_hubs_conf)
    .load()
    .selectExpr("CAST(body AS STRING) AS json_payload")
    ...
)
```

**Databricks Auto Loader (DBFS / S3 / ADLS):**
```python
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(BRONZE_SCHEMA)
    .load("abfss://container@account.dfs.core.windows.net/5g-qos/")
)
```

---

## 5G QoS KPI reference

| KPI | Good | Fair | Poor | Unit |
|-----|------|------|------|------|
| RSRP | ≥ −80 | −80 .. −100 | < −100 | dBm |
| RSRQ | ≥ −10 | −10 .. −15 | < −15 | dB |
| SINR | ≥ 10 | 0 .. 10 | < 0 | dB |
| CQI | ≥ 10 | 5 .. 9 | 0 .. 4 | — |
| DL Throughput (eMBB) | ≥ 200 | 50 .. 200 | < 50 | Mbps |
| Latency (URLLC) | ≤ 0.5 | 0.5 .. 1.0 | > 1.0 | ms |
| BLER | ≤ 2% | 2 .. 10% | > 10% | % |
| RB Utilisation | ≤ 60% | 60 .. 85% | > 85% | % |

---

## Licence

MIT — free to use, modify, and distribute for any purpose.

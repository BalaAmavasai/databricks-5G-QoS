"""
qos_simulator.py — Simulated 5G QoS event generator.

This module generates realistic 5G User Equipment (UE) quality-of-service
events for demonstration and testing purposes.  It can be called:

  1. Directly from Python (returns a list of dicts) for unit tests.
  2. From a Databricks notebook as a batch writer (writes to a Delta table).
  3. As part of a Spark streaming source via ``writeStream.foreachBatch``.

5G QoS parameters simulated
----------------------------
- RSRP  : Reference Signal Received Power   (-140 .. -44 dBm)
- RSRQ  : Reference Signal Received Quality (-19.5 .. -3 dB)
- SINR  : Signal-to-Interference-plus-Noise Ratio
- CQI   : Channel Quality Indicator          (0 .. 15)
- DL/UL throughput
- Latency, Jitter, Packet Loss
- MCS index, BLER, Resource Block utilisation

Network slices simulated
-------------------------
- S1 / eMBB   : Enhanced Mobile Broadband  (high throughput)
- S2 / URLLC  : Ultra-Reliable Low Latency (sub-ms latency)
- S3 / mMTC   : Massive Machine-Type Comms (IoT, low data)

Anomaly injection
------------------
A configurable fraction of events simulate degraded radio conditions
(handover failures, interference spikes, congested cells) to exercise
the alerting logic in the analytics notebooks.
"""

import uuid
import math
import random
import datetime
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Network topology constants
# ---------------------------------------------------------------------------

# Simulated UK city bounding boxes: (lat_min, lat_max, lon_min, lon_max)
_CITY_BBOXES: Dict[str, Tuple[float, float, float, float]] = {
    "London":     (51.40, 51.60, -0.25,  0.10),
    "Manchester": (53.44, 53.54, -2.30, -2.15),
    "Birmingham": (52.43, 52.53, -1.97, -1.80),
    "Leeds":      (53.77, 53.83, -1.62, -1.53),
    "Glasgow":    (55.83, 55.90, -4.33, -4.15),
}

_CITIES       = list(_CITY_BBOXES.keys())
_SLICE_INFO   = [
    {"id": "S1", "type": "eMBB"},
    {"id": "S2", "type": "URLLC"},
    {"id": "S3", "type": "mMTC"},
]


# ---------------------------------------------------------------------------
# Helper: bounded Gaussian sample
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _gauss(mean: float, std: float, lo: float, hi: float) -> float:
    return _clamp(random.gauss(mean, std), lo, hi)


# ---------------------------------------------------------------------------
# Per-slice baseline QoS profiles
# ---------------------------------------------------------------------------

_SLICE_PROFILES: Dict[str, Dict[str, Any]] = {
    "eMBB": {
        # Good signal in urban macro cells
        "rsrp_mean": -85.0,   "rsrp_std": 12.0,
        "rsrq_mean": -9.0,    "rsrq_std": 2.5,
        "sinr_mean": 15.0,    "sinr_std": 6.0,
        "cqi_mean": 10.5,     "cqi_std": 2.5,
        # High throughput
        "dl_tp_mean": 250.0,  "dl_tp_std": 80.0,
        "ul_tp_mean": 50.0,   "ul_tp_std": 20.0,
        # Moderate latency
        "lat_mean": 8.0,      "lat_std": 3.0,
        "jitter_mean": 2.0,   "jitter_std": 1.0,
        "pl_mean": 0.05,      "pl_std": 0.02,
        # Radio resource
        "mcs_mean": 20.0,     "mcs_std": 4.0,
        "bler_mean": 2.0,     "bler_std": 1.5,
        "rb_util_mean": 55.0, "rb_util_std": 20.0,
    },
    "URLLC": {
        # Dedicated URLLC slice with strict latency
        "rsrp_mean": -80.0,   "rsrp_std": 8.0,
        "rsrq_mean": -8.0,    "rsrq_std": 2.0,
        "sinr_mean": 18.0,    "sinr_std": 4.0,
        "cqi_mean": 12.0,     "cqi_std": 2.0,
        # Lower throughput (prioritise latency)
        "dl_tp_mean": 25.0,   "dl_tp_std": 10.0,
        "ul_tp_mean": 10.0,   "ul_tp_std": 5.0,
        # Very low latency
        "lat_mean": 0.5,      "lat_std": 0.15,
        "jitter_mean": 0.2,   "jitter_std": 0.08,
        "pl_mean": 0.0005,    "pl_std": 0.0002,
        # High-order modulation
        "mcs_mean": 24.0,     "mcs_std": 3.0,
        "bler_mean": 0.5,     "bler_std": 0.3,
        "rb_util_mean": 30.0, "rb_util_std": 10.0,
    },
    "mMTC": {
        # IoT devices with infrequent, small payloads
        "rsrp_mean": -105.0,  "rsrp_std": 15.0,
        "rsrq_mean": -14.0,   "rsrq_std": 3.0,
        "sinr_mean": 5.0,     "sinr_std": 5.0,
        "cqi_mean": 5.0,      "cqi_std": 2.5,
        # Very low throughput
        "dl_tp_mean": 2.0,    "dl_tp_std": 1.0,
        "ul_tp_mean": 0.5,    "ul_tp_std": 0.2,
        # Relaxed latency
        "lat_mean": 50.0,     "lat_std": 20.0,
        "jitter_mean": 15.0,  "jitter_std": 8.0,
        "pl_mean": 0.5,       "pl_std": 0.2,
        # Lower MCS (robust modulation for range)
        "mcs_mean": 8.0,      "mcs_std": 3.0,
        "bler_mean": 5.0,     "bler_std": 3.0,
        "rb_util_mean": 15.0, "rb_util_std": 8.0,
    },
}


# ---------------------------------------------------------------------------
# Anomaly profiles (injected at configurable rate)
# ---------------------------------------------------------------------------

def _apply_anomaly(event: Dict[str, Any], slice_type: str) -> Dict[str, Any]:
    """
    Randomly degrade one or more QoS dimensions to simulate:
    - Interference spike
    - Handover failure
    - Cell congestion
    - Backhaul degradation
    """
    anomaly_kind = random.choice(["interference", "handover", "congestion", "backhaul"])

    if anomaly_kind == "interference":
        # RSRP / SINR drop sharply
        event["rsrp_dbm"]  = str(round(_gauss(-120.0, 8.0, -140.0, -100.0), 2))
        event["sinr_db"]   = str(round(_gauss(-5.0,   4.0, -23.0,   2.0), 2))
        event["cqi"]       = str(random.randint(0, 3))
        event["bler_pct"]  = str(round(_gauss(18.0, 5.0, 10.0, 40.0), 2))

    elif anomaly_kind == "handover":
        # Temporary latency spike + packet loss during HO
        event["latency_ms"]      = str(round(_gauss(200.0, 50.0, 100.0, 500.0), 2))
        event["jitter_ms"]       = str(round(_gauss(30.0,  10.0, 15.0, 80.0), 2))
        event["packet_loss_pct"] = str(round(_gauss(3.0,   1.0,  1.0,  8.0), 4))
        event["handover_count"]  = str(random.randint(3, 10))

    elif anomaly_kind == "congestion":
        # RB utilisation saturated → throughput collapses
        event["rb_utilization_pct"] = str(round(_gauss(92.0, 4.0, 85.0, 100.0), 2))
        event["dl_throughput_mbps"] = str(round(_gauss(10.0, 5.0, 1.0, 30.0), 2))
        event["latency_ms"]         = str(round(_gauss(25.0, 8.0, 15.0, 60.0), 2))

    elif anomaly_kind == "backhaul":
        # Backhaul link degradation → latency + jitter
        event["latency_ms"]      = str(round(_gauss(80.0,  20.0, 50.0, 200.0), 2))
        event["jitter_ms"]       = str(round(_gauss(20.0,  8.0,  10.0, 60.0), 2))
        event["packet_loss_pct"] = str(round(_gauss(1.5,   0.5,  0.5,  5.0), 4))

    event["anomaly_kind"] = anomaly_kind  # extra field for debugging
    return event


# ---------------------------------------------------------------------------
# Core event generator
# ---------------------------------------------------------------------------

def generate_event(
    cell_id: str,
    gnb_id: str,
    city: str,
    anomaly_rate: float = 0.05,
    timestamp: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    """
    Generate a single simulated 5G UE QoS event.

    Parameters
    ----------
    cell_id : str
        Identifier of the serving cell (e.g. "CELL-LON-001").
    gnb_id : str
        Identifier of the parent gNodeB (5G base station).
    city : str
        City name used to pick a realistic lat/lon bounding box.
    anomaly_rate : float
        Probability (0–1) that this event simulates a degraded QoS condition.
    timestamp : datetime, optional
        Event timestamp.  If None, uses the current UTC time.

    Returns
    -------
    dict
        A flat dictionary of string values matching BRONZE_SCHEMA.
    """
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    slice_info  = random.choice(_SLICE_INFO)
    slice_type  = slice_info["type"]
    profile     = _SLICE_PROFILES[slice_type]
    bbox        = _CITY_BBOXES.get(city, _CITY_BBOXES["London"])

    is_anomaly = random.random() < anomaly_rate

    event: Dict[str, Any] = {
        # Identifiers
        "event_id":           str(uuid.uuid4()),
        "event_timestamp":    timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        "gnb_id":             gnb_id,
        "cell_id":            cell_id,
        "ue_id":              f"UE-{random.randint(1, 10000):05d}",
        "slice_id":           slice_info["id"],
        "slice_type":         slice_type,

        # Signal quality
        "rsrp_dbm":  str(round(_gauss(profile["rsrp_mean"], profile["rsrp_std"], -140.0, -44.0), 2)),
        "rsrq_db":   str(round(_gauss(profile["rsrq_mean"], profile["rsrq_std"], -19.5,  -3.0), 2)),
        "sinr_db":   str(round(_gauss(profile["sinr_mean"], profile["sinr_std"], -23.0,  40.0), 2)),
        "cqi":       str(int(_clamp(round(_gauss(profile["cqi_mean"], profile["cqi_std"], 0, 15)), 0, 15))),

        # Throughput
        "dl_throughput_mbps": str(round(abs(_gauss(profile["dl_tp_mean"], profile["dl_tp_std"], 0.1, 2000.0)), 2)),
        "ul_throughput_mbps": str(round(abs(_gauss(profile["ul_tp_mean"], profile["ul_tp_std"], 0.1, 1000.0)), 2)),

        # Latency
        "latency_ms":         str(round(abs(_gauss(profile["lat_mean"],    profile["lat_std"],    0.1, 1000.0)), 3)),
        "jitter_ms":          str(round(abs(_gauss(profile["jitter_mean"], profile["jitter_std"], 0.0, 200.0)),  3)),
        "packet_loss_pct":    str(round(abs(_gauss(profile["pl_mean"],     profile["pl_std"],     0.0, 10.0)),   5)),

        # Radio resources
        "mcs_index":          str(int(_clamp(round(_gauss(profile["mcs_mean"], profile["mcs_std"], 0, 28)), 0, 28))),
        "bler_pct":           str(round(abs(_gauss(profile["bler_mean"],    profile["bler_std"],   0.0, 50.0)), 2)),
        "rb_utilization_pct": str(round(abs(_gauss(profile["rb_util_mean"], profile["rb_util_std"],0.0, 100.0)),2)),

        # Location
        "latitude":  str(round(random.uniform(bbox[0], bbox[1]), 6)),
        "longitude": str(round(random.uniform(bbox[2], bbox[3]), 6)),

        # Session
        "handover_count": str(random.randint(0, 2)),
        "is_anomaly":     str(is_anomaly).lower(),
    }

    if is_anomaly:
        event = _apply_anomaly(event, slice_type)

    return event


# ---------------------------------------------------------------------------
# Batch generator — produces N events across a configured topology
# ---------------------------------------------------------------------------

def generate_batch(
    num_cells: int = 20,
    events_per_batch: int = 50,
    anomaly_rate: float = 0.05,
    timestamp: Optional[datetime.datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Generate a batch of simulated 5G QoS events spread across ``num_cells``
    cells and several UK cities.

    Parameters
    ----------
    num_cells : int
        Number of cells to distribute events across.
    events_per_batch : int
        Total events to generate in this batch.
    anomaly_rate : float
        Fraction of events with degraded QoS.
    timestamp : datetime, optional
        Timestamp for all events in this batch (defaults to utcnow).

    Returns
    -------
    list of dict
        Flat dictionaries matching BRONZE_SCHEMA.
    """
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    # Build a stable set of cells for this simulation run
    cells = []
    for i in range(num_cells):
        city   = _CITIES[i % len(_CITIES)]
        gnb_id = f"GNB-{city[:3].upper()}-{(i // len(_CITIES)) + 1:03d}"
        cell_id = f"CELL-{city[:3].upper()}-{i:03d}"
        cells.append((cell_id, gnb_id, city))

    events = []
    for _ in range(events_per_batch):
        cell_id, gnb_id, city = random.choice(cells)
        # Add slight sub-second jitter so each event has a unique timestamp
        ts = timestamp + datetime.timedelta(milliseconds=random.randint(0, 999))
        events.append(generate_event(cell_id, gnb_id, city, anomaly_rate, ts))

    return events


# ---------------------------------------------------------------------------
# Convenience: classify signal quality from RSRP
# ---------------------------------------------------------------------------

def classify_signal_quality(rsrp_dbm: Optional[float]) -> str:
    """
    Return a human-readable RSRP quality class per 3GPP TS 38.133.

    ≥ -80  dBm  → Excellent
    -80 .. -90  → Good
    -90 .. -100 → Fair
    < -100 dBm  → Poor
    """
    if rsrp_dbm is None:
        return "Unknown"
    if rsrp_dbm >= -80:
        return "Excellent"
    if rsrp_dbm >= -90:
        return "Good"
    if rsrp_dbm >= -100:
        return "Fair"
    return "Poor"


# ---------------------------------------------------------------------------
# SLA breach detection (used by the silver-layer UDF)
# ---------------------------------------------------------------------------

_SLA_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "eMBB": {
        "min_dl_throughput_mbps": 100.0,
        "min_ul_throughput_mbps": 20.0,
        "max_latency_ms":         10.0,
        "max_packet_loss_pct":    0.1,
        "max_jitter_ms":          5.0,
    },
    "URLLC": {
        "min_dl_throughput_mbps": 10.0,
        "min_ul_throughput_mbps": 5.0,
        "max_latency_ms":         1.0,
        "max_packet_loss_pct":    0.001,
        "max_jitter_ms":          0.5,
    },
    "mMTC": {
        "min_dl_throughput_mbps": 1.0,
        "min_ul_throughput_mbps": 0.5,
        "max_latency_ms":         100.0,
        "max_packet_loss_pct":    1.0,
        "max_jitter_ms":          50.0,
    },
}


def check_sla_breach(
    slice_type: Optional[str],
    dl_throughput: Optional[float],
    ul_throughput: Optional[float],
    latency_ms: Optional[float],
    packet_loss_pct: Optional[float],
    jitter_ms: Optional[float],
) -> bool:
    """
    Return True if any QoS metric violates the SLA for the given slice type.

    Parameters match the silver schema field names.  Any None value is treated
    as not breaching (i.e. the check is skipped for missing metrics).
    """
    if slice_type not in _SLA_THRESHOLDS:
        return False

    sla = _SLA_THRESHOLDS[slice_type]

    if dl_throughput  is not None and dl_throughput  < sla["min_dl_throughput_mbps"]:
        return True
    if ul_throughput  is not None and ul_throughput  < sla["min_ul_throughput_mbps"]:
        return True
    if latency_ms     is not None and latency_ms     > sla["max_latency_ms"]:
        return True
    if packet_loss_pct is not None and packet_loss_pct > sla["max_packet_loss_pct"]:
        return True
    if jitter_ms      is not None and jitter_ms      > sla["max_jitter_ms"]:
        return True

    return False

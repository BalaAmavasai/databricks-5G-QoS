"""
test_simulator.py — Unit tests for the 5G QoS data simulator.

Run locally (no Spark required) with:
    pip install pytest
    pytest tests/ -v

The simulator module uses only Python standard library, so tests run
without a Databricks cluster.
"""

import sys
import os
import datetime
import pytest

# Add src/ to the path so we can import without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from qos_simulator import (
    generate_event,
    generate_batch,
    classify_signal_quality,
    check_sla_breach,
    _clamp,
    _SLICE_INFO,
)

# ---------------------------------------------------------------------------
# Bronze schema field names (mirrors BRONZE_SCHEMA in src/schema.py).
# Defined here so pyspark is not required for local unit tests.
# ---------------------------------------------------------------------------
BRONZE_FIELD_NAMES = {
    "event_id", "event_timestamp", "gnb_id", "cell_id", "ue_id",
    "slice_id", "slice_type", "rsrp_dbm", "rsrq_db", "sinr_db", "cqi",
    "dl_throughput_mbps", "ul_throughput_mbps", "latency_ms", "jitter_ms",
    "packet_loss_pct", "mcs_index", "bler_pct", "rb_utilization_pct",
    "latitude", "longitude", "handover_count", "is_anomaly",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXED_TS = datetime.datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture
def single_event():
    """Return a deterministic single event for London CELL-LON-000."""
    import random
    random.seed(42)
    return generate_event(
        cell_id="CELL-LON-000",
        gnb_id="GNB-LON-001",
        city="London",
        anomaly_rate=0.0,      # No anomalies for deterministic tests
        timestamp=FIXED_TS,
    )


@pytest.fixture
def anomaly_event():
    """Return an event guaranteed to be an anomaly."""
    import random
    random.seed(99)
    # Set anomaly_rate=1.0 to guarantee an anomaly
    return generate_event(
        cell_id="CELL-MAN-000",
        gnb_id="GNB-MAN-001",
        city="Manchester",
        anomaly_rate=1.0,
        timestamp=FIXED_TS,
    )


# ---------------------------------------------------------------------------
# test_generate_event — field presence and types
# ---------------------------------------------------------------------------

class TestGenerateEvent:

    EXPECTED_STRING_FIELDS = [
        "event_id", "event_timestamp", "gnb_id", "cell_id", "ue_id",
        "slice_id", "slice_type", "rsrp_dbm", "rsrq_db", "sinr_db",
        "cqi", "dl_throughput_mbps", "ul_throughput_mbps", "latency_ms",
        "jitter_ms", "packet_loss_pct", "mcs_index", "bler_pct",
        "rb_utilization_pct", "latitude", "longitude", "handover_count",
        "is_anomaly",
    ]

    def test_all_schema_fields_present(self, single_event):
        """Every field in BRONZE_FIELD_NAMES should exist in the generated event."""
        for name in BRONZE_FIELD_NAMES:
            assert name in single_event, f"Missing field: {name}"

    def test_all_values_are_strings(self, single_event):
        """All values should be strings to match the BRONZE_SCHEMA StringType."""
        for field in self.EXPECTED_STRING_FIELDS:
            assert isinstance(single_event[field], str), (
                f"Field '{field}' is {type(single_event[field])}, expected str"
            )

    def test_identifiers_are_non_empty(self, single_event):
        for field in ("event_id", "gnb_id", "cell_id", "ue_id"):
            assert single_event[field], f"Field '{field}' should not be empty"

    def test_cell_and_gnb_match_input(self, single_event):
        assert single_event["cell_id"] == "CELL-LON-000"
        assert single_event["gnb_id"]  == "GNB-LON-001"

    def test_timestamp_format(self, single_event):
        """Timestamp should be an ISO-8601 string ending with Z."""
        ts = single_event["event_timestamp"]
        assert ts.endswith("Z"), f"Timestamp '{ts}' should end with Z"
        # Must be parseable
        parsed = datetime.datetime.fromisoformat(ts.rstrip("Z"))
        assert parsed.date() == FIXED_TS.date()

    def test_slice_id_valid(self, single_event):
        valid_ids = {s["id"] for s in _SLICE_INFO}
        assert single_event["slice_id"] in valid_ids, (
            f"slice_id '{single_event['slice_id']}' not in {valid_ids}"
        )

    def test_slice_type_valid(self, single_event):
        valid_types = {"eMBB", "URLLC", "mMTC"}
        assert single_event["slice_type"] in valid_types

    def test_rsrp_in_valid_range(self, single_event):
        rsrp = float(single_event["rsrp_dbm"])
        assert -140.0 <= rsrp <= -44.0, f"RSRP {rsrp} out of range"

    def test_cqi_in_valid_range(self, single_event):
        cqi = int(single_event["cqi"])
        assert 0 <= cqi <= 15, f"CQI {cqi} out of 0..15 range"

    def test_mcs_in_valid_range(self, single_event):
        mcs = int(single_event["mcs_index"])
        assert 0 <= mcs <= 28, f"MCS {mcs} out of 0..28 range"

    def test_throughput_positive(self, single_event):
        assert float(single_event["dl_throughput_mbps"]) > 0
        assert float(single_event["ul_throughput_mbps"]) > 0

    def test_latency_positive(self, single_event):
        assert float(single_event["latency_ms"]) > 0

    def test_packet_loss_non_negative(self, single_event):
        assert float(single_event["packet_loss_pct"]) >= 0

    def test_rb_utilization_in_range(self, single_event):
        rb = float(single_event["rb_utilization_pct"])
        assert 0.0 <= rb <= 100.0, f"RB util {rb} out of 0..100 range"

    def test_is_anomaly_false_when_rate_zero(self, single_event):
        assert single_event["is_anomaly"] == "false"

    def test_latitude_in_uk(self, single_event):
        lat = float(single_event["latitude"])
        # UK bounding box: ~49.9 – 60.9
        assert 49.0 <= lat <= 61.0, f"Latitude {lat} out of UK bounds"

    def test_longitude_in_uk(self, single_event):
        lon = float(single_event["longitude"])
        assert -8.0 <= lon <= 2.0, f"Longitude {lon} out of UK bounds"


# ---------------------------------------------------------------------------
# test_anomaly_event
# ---------------------------------------------------------------------------

class TestAnomalyEvent:

    def test_is_anomaly_true(self, anomaly_event):
        assert anomaly_event["is_anomaly"] == "true"

    def test_anomaly_degrades_at_least_one_metric(self, anomaly_event):
        """An anomaly event should have at least one degraded metric."""
        rsrp = float(anomaly_event["rsrp_dbm"])
        latency = float(anomaly_event["latency_ms"])
        rb_util = float(anomaly_event["rb_utilization_pct"])
        pkt_loss = float(anomaly_event["packet_loss_pct"])

        degraded = (
            rsrp < -100
            or latency > 15
            or rb_util > 85
            or pkt_loss > 0.5
        )
        assert degraded, "Anomaly event should degrade at least one QoS metric"


# ---------------------------------------------------------------------------
# test_generate_batch
# ---------------------------------------------------------------------------

class TestGenerateBatch:

    def test_batch_size(self):
        batch = generate_batch(num_cells=5, events_per_batch=20, anomaly_rate=0.0)
        assert len(batch) == 20

    def test_all_events_have_schema_fields(self):
        batch = generate_batch(num_cells=5, events_per_batch=10, anomaly_rate=0.0)
        for event in batch:
            for field in BRONZE_FIELD_NAMES:
                assert field in event, f"Batch event missing field: {field}"

    def test_batch_contains_multiple_cells(self):
        batch = generate_batch(num_cells=10, events_per_batch=100, anomaly_rate=0.0)
        cells = {e["cell_id"] for e in batch}
        # With 10 cells and 100 events, we should see more than 1 cell
        assert len(cells) > 1

    def test_anomaly_rate_roughly_correct(self):
        import random
        random.seed(0)
        batch = generate_batch(
            num_cells=5, events_per_batch=1000, anomaly_rate=0.1
        )
        anomaly_count = sum(1 for e in batch if e["is_anomaly"] == "true")
        # Allow ±5% tolerance
        assert 50 <= anomaly_count <= 150, (
            f"Anomaly count {anomaly_count} not near expected 100 (10%)"
        )

    def test_batch_zero_events(self):
        batch = generate_batch(num_cells=5, events_per_batch=0, anomaly_rate=0.0)
        assert batch == []

    def test_single_cell_batch(self):
        batch = generate_batch(num_cells=1, events_per_batch=10, anomaly_rate=0.0)
        cells = {e["cell_id"] for e in batch}
        assert len(cells) == 1

    def test_all_slice_types_present_in_large_batch(self):
        import random
        random.seed(1)
        batch = generate_batch(num_cells=5, events_per_batch=300, anomaly_rate=0.0)
        slice_types = {e["slice_type"] for e in batch}
        assert "eMBB"  in slice_types
        assert "URLLC" in slice_types
        assert "mMTC"  in slice_types


# ---------------------------------------------------------------------------
# test_classify_signal_quality
# ---------------------------------------------------------------------------

class TestClassifySignalQuality:

    def test_excellent(self):
        assert classify_signal_quality(-75.0) == "Excellent"

    def test_good(self):
        assert classify_signal_quality(-85.0) == "Good"

    def test_fair(self):
        assert classify_signal_quality(-95.0) == "Fair"

    def test_poor(self):
        assert classify_signal_quality(-105.0) == "Poor"

    def test_none_returns_unknown(self):
        assert classify_signal_quality(None) == "Unknown"

    def test_boundary_excellent_good(self):
        # Exactly -80 should be Excellent (rsrp >= -80)
        assert classify_signal_quality(-80.0) == "Excellent"

    def test_boundary_good_fair(self):
        # Exactly -90 should be Good (rsrp >= -90)
        assert classify_signal_quality(-90.0) == "Good"

    def test_boundary_fair_poor(self):
        # Exactly -100 should be Fair (rsrp >= -100)
        assert classify_signal_quality(-100.0) == "Fair"

    def test_just_below_fair_poor_boundary(self):
        assert classify_signal_quality(-100.1) == "Poor"


# ---------------------------------------------------------------------------
# test_check_sla_breach
# ---------------------------------------------------------------------------

class TestCheckSlaBreach:

    # ── eMBB SLA: DL≥100, UL≥20, lat≤10, pkt_loss≤0.1, jitter≤5 ─────────

    def test_embb_no_breach(self):
        assert not check_sla_breach("eMBB", 200.0, 25.0, 8.0, 0.05, 3.0)

    def test_embb_low_dl_throughput(self):
        assert check_sla_breach("eMBB", 50.0, 25.0, 8.0, 0.05, 3.0)

    def test_embb_low_ul_throughput(self):
        assert check_sla_breach("eMBB", 200.0, 10.0, 8.0, 0.05, 3.0)

    def test_embb_high_latency(self):
        assert check_sla_breach("eMBB", 200.0, 25.0, 15.0, 0.05, 3.0)

    def test_embb_high_packet_loss(self):
        assert check_sla_breach("eMBB", 200.0, 25.0, 8.0, 0.5, 3.0)

    def test_embb_high_jitter(self):
        assert check_sla_breach("eMBB", 200.0, 25.0, 8.0, 0.05, 10.0)

    # ── URLLC SLA: lat≤1ms ─────────────────────────────────────────────────

    def test_urllc_no_breach(self):
        assert not check_sla_breach("URLLC", 15.0, 6.0, 0.8, 0.0001, 0.3)

    def test_urllc_high_latency(self):
        assert check_sla_breach("URLLC", 15.0, 6.0, 2.0, 0.0001, 0.3)

    def test_urllc_boundary_latency(self):
        # Exactly at threshold — should not breach (> not >=)
        assert not check_sla_breach("URLLC", 15.0, 6.0, 1.0, 0.0001, 0.3)

    def test_urllc_just_above_threshold(self):
        assert check_sla_breach("URLLC", 15.0, 6.0, 1.001, 0.0001, 0.3)

    # ── mMTC SLA ───────────────────────────────────────────────────────────

    def test_mmtc_no_breach(self):
        assert not check_sla_breach("mMTC", 2.0, 1.0, 50.0, 0.5, 20.0)

    def test_mmtc_high_latency(self):
        assert check_sla_breach("mMTC", 2.0, 1.0, 150.0, 0.5, 20.0)

    # ── Edge cases ─────────────────────────────────────────────────────────

    def test_unknown_slice_no_breach(self):
        # Unknown slice type: no SLA thresholds → no breach
        assert not check_sla_breach("Unknown", 0.1, 0.1, 9999.0, 99.0, 999.0)

    def test_none_slice_no_breach(self):
        assert not check_sla_breach(None, 0.1, 0.1, 9999.0, 99.0, 999.0)

    def test_none_kpi_values_skip_check(self):
        # If all KPIs are None, no breach should be declared
        assert not check_sla_breach("eMBB", None, None, None, None, None)

    def test_partial_none_kpis(self):
        # Only latency is None; DL throughput is below threshold → breach
        assert check_sla_breach("eMBB", 50.0, None, None, None, None)


# ---------------------------------------------------------------------------
# test__clamp helper
# ---------------------------------------------------------------------------

class TestClamp:

    def test_clamp_within_range(self):
        assert _clamp(5.0, 0.0, 10.0) == 5.0

    def test_clamp_at_lower(self):
        assert _clamp(-5.0, 0.0, 10.0) == 0.0

    def test_clamp_at_upper(self):
        assert _clamp(15.0, 0.0, 10.0) == 10.0

    def test_clamp_exactly_lower(self):
        assert _clamp(0.0, 0.0, 10.0) == 0.0

    def test_clamp_exactly_upper(self):
        assert _clamp(10.0, 0.0, 10.0) == 10.0

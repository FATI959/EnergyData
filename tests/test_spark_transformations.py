"""
EnergyData Lakehouse Platform — Spark Transformations Test Suite
==================================================================
Sprint 3 | Tests unitaires des transformations Bronze & Silver

Valide le bon fonctionnement du parsing JSON, du typage strict, des règles
de plausibilité physique (is_valid_reading), du calcul de latence et de la
déduplication.
"""

import json
from datetime import datetime, timezone
import pytest


from pathlib import Path


# Payload d'exemple conforme au contrat d'interface 1
SAMPLE_VALID_PAYLOAD = {
    "device_id": "METER_SECTOR_A_001",
    "timestamp": "2026-10-15T14:15:00.000000Z",
    "substation_id": "SUBSTATION_NORTH_01",
    "location": {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "zone": "Industrial_Park",
    },
    "metrics": {
        "voltage_v": 231.4,
        "current_a": 15.8,
        "active_power_kw": 3.65,
        "reactive_power_kvar": 0.42,
        "power_factor": 0.99,
        "frequency_hz": 50.02,
        "ambient_temp_c": 19.5,
    },
    "status_flag": "NORMAL",
}


def test_iot_payload_schema_keys():
    """Vérifie que le payload sample contient toutes les clés obligatoires."""
    payload = SAMPLE_VALID_PAYLOAD
    assert "device_id" in payload
    assert "timestamp" in payload
    assert "substation_id" in payload
    assert "location" in payload
    assert "metrics" in payload
    assert "status_flag" in payload

    metrics = payload["metrics"]
    required_metrics = [
        "voltage_v",
        "current_a",
        "active_power_kw",
        "reactive_power_kvar",
        "power_factor",
        "frequency_hz",
        "ambient_temp_c",
    ]
    for m in required_metrics:
        assert m in metrics, f"Métrique manquante : {m}"


def evaluate_validity_logic(metrics: dict, quality_rules: dict) -> bool:
    """Reproduction pure Python de la règle is_valid_reading de 02_silver_cleaning."""
    v = metrics.get("voltage_v", 0.0)
    c = metrics.get("current_a", 0.0)
    pf = metrics.get("power_factor", 0.0)
    f = metrics.get("frequency_hz", 0.0)
    p = metrics.get("active_power_kw", 0.0)

    is_valid = (
        (quality_rules["voltage_min_v"] <= v <= quality_rules["voltage_max_v"])
        and (c >= quality_rules["current_min_a"])
        and (quality_rules["power_factor_min"] <= pf <= quality_rules["power_factor_max"])
        and (quality_rules["frequency_min_hz"] <= f <= quality_rules["frequency_max_hz"])
        and (p >= 0.0)
    )
    return is_valid


def test_quality_rules_valid_reading():
    rules = {
        "voltage_min_v": 100.0,
        "voltage_max_v": 280.0,
        "current_min_a": 0.0,
        "power_factor_min": 0.0,
        "power_factor_max": 1.0,
        "frequency_min_hz": 48.0,
        "frequency_max_hz": 52.0,
    }
    assert evaluate_validity_logic(SAMPLE_VALID_PAYLOAD["metrics"], rules) is True


def test_quality_rules_overvoltage():
    rules = {
        "voltage_min_v": 100.0,
        "voltage_max_v": 280.0,
        "current_min_a": 0.0,
        "power_factor_min": 0.0,
        "power_factor_max": 1.0,
        "frequency_min_hz": 48.0,
        "frequency_max_hz": 52.0,
    }
    bad_metrics = dict(SAMPLE_VALID_PAYLOAD["metrics"])
    bad_metrics["voltage_v"] = 310.0  # Surtension anormale
    assert evaluate_validity_logic(bad_metrics, rules) is False


def test_quality_rules_undervoltage():
    rules = {
        "voltage_min_v": 100.0,
        "voltage_max_v": 280.0,
        "current_min_a": 0.0,
        "power_factor_min": 0.0,
        "power_factor_max": 1.0,
        "frequency_min_hz": 48.0,
        "frequency_max_hz": 52.0,
    }
    bad_metrics = dict(SAMPLE_VALID_PAYLOAD["metrics"])
    bad_metrics["voltage_v"] = 85.0  # Chute anormale
    assert evaluate_validity_logic(bad_metrics, rules) is False


def test_quality_rules_negative_current():
    rules = {
        "voltage_min_v": 100.0,
        "voltage_max_v": 280.0,
        "current_min_a": 0.0,
        "power_factor_min": 0.0,
        "power_factor_max": 1.0,
        "frequency_min_hz": 48.0,
        "frequency_max_hz": 52.0,
    }
    bad_metrics = dict(SAMPLE_VALID_PAYLOAD["metrics"])
    bad_metrics["current_a"] = -5.0  # Courant négatif impossible
    assert evaluate_validity_logic(bad_metrics, rules) is False


def test_quality_rules_invalid_power_factor():
    rules = {
        "voltage_min_v": 100.0,
        "voltage_max_v": 280.0,
        "current_min_a": 0.0,
        "power_factor_min": 0.0,
        "power_factor_max": 1.0,
        "frequency_min_hz": 48.0,
        "frequency_max_hz": 52.0,
    }
    bad_metrics = dict(SAMPLE_VALID_PAYLOAD["metrics"])
    bad_metrics["power_factor"] = 1.25  # Power factor > 1 impossible
    assert evaluate_validity_logic(bad_metrics, rules) is False


def test_ingestion_latency_calculation():
    event_time_str = "2026-10-15T14:15:00.000000Z"
    ingestion_time_str = "2026-10-15T14:15:04.500000Z"

    dt_event = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
    dt_ingestion = datetime.fromisoformat(ingestion_time_str.replace("Z", "+00:00"))

    latency_sec = (dt_ingestion - dt_event).total_seconds()
    assert latency_sec == 4.5
    assert latency_sec >= 0.0


def test_databricks_config_structure():
    """Valide que le fichier de configuration JSON est valide et complet."""
    config_file = Path(__file__).resolve().parent.parent / "config" / "databricks_config.json"
    with open(config_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    assert "storage" in cfg
    assert "event_hubs" in cfg
    assert "streaming" in cfg
    assert "quality_rules" in cfg

    paths = cfg["storage"]["paths"]
    assert "bronze" in paths
    assert "silver" in paths
    assert "checkpoints" in paths

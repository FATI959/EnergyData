"""
EnergyData Lakehouse Platform — Data Quality & Quarantine Test Suite
======================================================================
Sprint 4 | Tests unitaires du moteur de gouvernance et de la table Quarantaine (DLQ)

Valide les règles de contrôle physique, l'étiquetage des motifs d'erreur (quarantine_reason),
la sévérité des rejets et le respect des SLAs de qualité (>= 98%).
"""

import json
from pathlib import Path


def evaluate_record_quality(reading: dict, rules: dict) -> dict:
    """
    Simule la logique métier de 03_data_quality_checks.py en Python pur
    pour valider l'intégrité sans dépendre de l'infrastructure Spark active.
    """
    v = reading.get("voltage_v", 0.0)
    c = reading.get("current_a", 0.0)
    pf = reading.get("power_factor", 0.0)
    f = reading.get("frequency_hz", 0.0)
    p = reading.get("active_power_kw", 0.0)

    reasons = []
    if v < rules.get("voltage_min_v", 100.0) or v > rules.get("voltage_max_v", 280.0):
        reasons.append("VOLTAGE_OUT_OF_BOUNDS")
    if f < rules.get("frequency_min_hz", 48.0) or f > rules.get("frequency_max_hz", 52.0):
        reasons.append("FREQUENCY_OUT_OF_BOUNDS")
    if pf < rules.get("power_factor_min", 0.0) or pf > rules.get("power_factor_max", 1.0):
        reasons.append("INVALID_POWER_FACTOR")
    if c < rules.get("current_min_a", 0.0) or c > rules.get("current_max_a", 250.0):
        reasons.append("INVALID_CURRENT")
    if p < 0.0:
        reasons.append("NEGATIVE_ACTIVE_POWER")

    is_valid = len(reasons) == 0
    severity = "PASSED"
    if not is_valid:
        if "VOLTAGE_OUT_OF_BOUNDS" in reasons or "FREQUENCY_OUT_OF_BOUNDS" in reasons:
            severity = "CRITICAL"
        else:
            severity = "WARNING"

    return {
        "is_valid": is_valid,
        "quarantine_reason": "; ".join(reasons) if reasons else None,
        "severity": severity,
    }


def compute_sla(valid_count: int, total_count: int, target: float = 98.0) -> dict:
    pct = round((valid_count / total_count * 100.0), 2) if total_count > 0 else 100.0
    return {
        "compliance_pct": pct,
        "is_compliant": pct >= target,
    }


# ---------------------------------------------------------------------------
# Tests unitaires
# ---------------------------------------------------------------------------
DEFAULT_RULES = {
    "voltage_min_v": 100.0,
    "voltage_max_v": 280.0,
    "current_min_a": 0.0,
    "current_max_a": 250.0,
    "power_factor_min": 0.0,
    "power_factor_max": 1.0,
    "frequency_min_hz": 48.0,
    "frequency_max_hz": 52.0,
    "sla_target_percent": 98.0,
}


def test_nominal_reading_passes():
    sample = {
        "voltage_v": 230.5,
        "current_a": 12.0,
        "active_power_kw": 2.76,
        "power_factor": 0.98,
        "frequency_hz": 50.01,
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is True
    assert result["quarantine_reason"] is None
    assert result["severity"] == "PASSED"


def test_overvoltage_tagged_as_critical():
    sample = {
        "voltage_v": 320.0,  # Surtension critique
        "current_a": 15.0,
        "active_power_kw": 4.5,
        "power_factor": 0.95,
        "frequency_hz": 50.0,
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is False
    assert "VOLTAGE_OUT_OF_BOUNDS" in result["quarantine_reason"]
    assert result["severity"] == "CRITICAL"


def test_frequency_deviation_tagged_as_critical():
    sample = {
        "voltage_v": 230.0,
        "current_a": 10.0,
        "active_power_kw": 2.3,
        "power_factor": 0.95,
        "frequency_hz": 46.5,  # Chute de fréquence dangereuse
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is False
    assert "FREQUENCY_OUT_OF_BOUNDS" in result["quarantine_reason"]
    assert result["severity"] == "CRITICAL"


def test_invalid_power_factor_warning():
    sample = {
        "voltage_v": 230.0,
        "current_a": 10.0,
        "active_power_kw": 2.3,
        "power_factor": 1.15,  # PF impossible (> 1)
        "frequency_hz": 50.0,
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is False
    assert "INVALID_POWER_FACTOR" in result["quarantine_reason"]
    assert result["severity"] == "WARNING"


def test_negative_active_power():
    sample = {
        "voltage_v": 230.0,
        "current_a": 5.0,
        "active_power_kw": -1.5,  # Puissance négative anormale
        "power_factor": 0.9,
        "frequency_hz": 50.0,
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is False
    assert "NEGATIVE_ACTIVE_POWER" in result["quarantine_reason"]


def test_multiple_faults_aggregated():
    sample = {
        "voltage_v": 90.0,  # Sous-tension
        "current_a": -2.0,  # Courant négatif
        "active_power_kw": -0.5,
        "power_factor": 1.3,  # PF > 1
        "frequency_hz": 54.0,  # Sur-fréquence
    }
    result = evaluate_record_quality(sample, DEFAULT_RULES)
    assert result["is_valid"] is False
    assert "VOLTAGE_OUT_OF_BOUNDS" in result["quarantine_reason"]
    assert "FREQUENCY_OUT_OF_BOUNDS" in result["quarantine_reason"]
    assert "INVALID_CURRENT" in result["quarantine_reason"]
    assert result["severity"] == "CRITICAL"


def test_sla_compliance_pass():
    res = compute_sla(valid_count=985, total_count=1000, target=98.0)
    assert res["compliance_pct"] == 98.5
    assert res["is_compliant"] is True


def test_sla_compliance_violation():
    res = compute_sla(valid_count=950, total_count=1000, target=98.0)
    assert res["compliance_pct"] == 95.0
    assert res["is_compliant"] is False


def test_quality_rules_in_config():
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "databricks_config.json"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)
    assert "quality_rules" in cfg
    qr = cfg["quality_rules"]
    assert "voltage_min_v" in qr
    assert "voltage_max_v" in qr
    assert "frequency_min_hz" in qr


"""
Stream scoring module for Anomaly Detection.
Sprint 6A - ML Engine
Formats detected anomalies for Delta table 'gold_detected_anomalies'.
"""

import logging
import uuid
from typing import Any

import numpy as np
import pandas as pd

from ml_engine.anomaly_detection.train_detector import (
    DEFAULT_FEATURES,
    classify_severity,
    determine_anomaly_type,
    load_model,
)

logger = logging.getLogger("stream_scoring")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def score_micro_batch(
    df: pd.DataFrame,
    model_or_path: Any,
    features: list[str] | None = None,
) -> pd.DataFrame:
    """
    Scores a pandas micro-batch of telemetry readings.

    Filters for anomalous readings and returns them formatted
    according to the gold_detected_anomalies contract:
    - anomaly_id: UUID
    - device_id: string
    - timestamp: datetime / string
    - anomaly_type: string
    - severity: string (Low, Medium, Critical)
    - z_score: double
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "anomaly_id",
                "device_id",
                "timestamp",
                "anomaly_type",
                "severity",
                "z_score",
            ]
        )

    if isinstance(model_or_path, str):
        model = load_model(model_or_path)
    else:
        model = model_or_path

    if features is None:
        features = [col for col in DEFAULT_FEATURES if col in df.columns]

    X = df[features].fillna(0.0)
    predictions = model.predict(X)
    decision_scores = model.decision_function(X)

    anomalies_idx = np.where(predictions == -1)[0]

    if len(anomalies_idx) == 0:
        return pd.DataFrame(
            columns=[
                "anomaly_id",
                "device_id",
                "timestamp",
                "anomaly_type",
                "severity",
                "z_score",
            ]
        )

    results: list[dict[str, Any]] = []
    for idx in anomalies_idx:
        row = df.iloc[idx]
        score = float(decision_scores[idx])
        z = float(row["z_score_power"]) if "z_score_power" in row and pd.notnull(row["z_score_power"]) else 0.0

        severity = classify_severity(score, z)

        v = float(row["voltage_v"]) if "voltage_v" in row and pd.notnull(row["voltage_v"]) else None
        freq = float(row["frequency_hz"]) if "frequency_hz" in row and pd.notnull(row["frequency_hz"]) else None
        pf = float(row["power_factor"]) if "power_factor" in row and pd.notnull(row["power_factor"]) else None

        anom_type = determine_anomaly_type(
            voltage=v,
            frequency=freq,
            power_factor=pf,
            z_score=z,
        )

        device_id = str(row["device_id"]) if "device_id" in row else (
            str(row["substation_id"]) if "substation_id" in row else "unknown_device"
        )
        ts = row["timestamp"] if "timestamp" in row else (
            row["window_start"] if "window_start" in row else pd.Timestamp.now()
        )

        results.append(
            {
                "anomaly_id": str(uuid.uuid4()),
                "device_id": device_id,
                "timestamp": ts,
                "anomaly_type": anom_type,
                "severity": severity,
                "z_score": round(z, 4),
            }
        )

    return pd.DataFrame(results)

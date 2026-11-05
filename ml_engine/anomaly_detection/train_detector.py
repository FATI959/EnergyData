"""
Anomaly Detection Model Training with Isolation Forest.
Sprint 6A - ML Engine
"""

import logging
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("train_detector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DEFAULT_FEATURES = [
    "voltage_v",
    "current_a",
    "active_power_kw",
    "power_factor",
    "frequency_hz",
]

GOLD_FEATURES = [
    "avg_active_power_kw",
    "peak_active_power_kw",
    "avg_power_factor",
    "voltage_range",
    "power_factor_deviation",
    "z_score_power",
]


def classify_severity(anomaly_score: float, z_score: float = 0.0) -> str:
    """
    Classifies anomaly severity into Low, Medium, or Critical.

    Args:
        anomaly_score: Isolation Forest decision function score (lower is more anomalous).
        z_score: Absolute or signed statistical z-score.

    Returns:
        One of 'Low', 'Medium', 'Critical'.
    """
    abs_z = abs(z_score)
    if abs_z >= 4.0 or anomaly_score < -0.25:
        return "Critical"
    elif abs_z >= 3.0 or anomaly_score < -0.10:
        return "Medium"
    else:
        return "Low"


def determine_anomaly_type(
    voltage: float | None = None,
    frequency: float | None = None,
    power_factor: float | None = None,
    z_score: float = 0.0,
) -> str:
    """
    Infers physical anomaly type based on domain rules.
    """
    if voltage is not None and (voltage < 207.0 or voltage > 253.0):
        return "voltage_drop" if voltage < 207.0 else "voltage_spike"
    if frequency is not None and (frequency < 49.5 or frequency > 50.5):
        return "frequency_drift"
    if power_factor is not None and power_factor < 0.85:
        return "power_factor_degradation"
    if abs(z_score) >= 3.0:
        return "load_surge" if z_score > 0 else "power_drop"
    return "general_anomaly"


def train_isolation_forest(
    data: pd.DataFrame,
    features: list[str] | None = None,
    contamination: float = 0.03,
    n_estimators: int = 150,
    random_state: int = 42,
) -> tuple[IsolationForest, list[str]]:
    """
    Trains an Isolation Forest model on provided features.

    Args:
        data: DataFrame containing input training telemetry.
        features: List of feature column names.
        contamination: Proportion of expected outliers.
        n_estimators: Number of trees.
        random_state: Random seed for reproducibility.

    Returns:
        Trained IsolationForest model and list of features used.
    """
    if features is None:
        features = [col for col in DEFAULT_FEATURES if col in data.columns]
        if not features:
            features = [col for col in GOLD_FEATURES if col in data.columns]

    if not features:
        raise ValueError("No valid feature columns found in dataset.")

    missing = [f for f in features if f not in data.columns]
    if missing:
        raise ValueError(f"Features missing from DataFrame: {missing}")

    X = data[features].fillna(0.0)

    logger.info("Training Isolation Forest on %d samples with %d features: %s", len(X), len(features), features)
    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(X)
    logger.info("Isolation Forest training complete.")
    return model, features


def predict_anomalies(
    model: IsolationForest,
    data: pd.DataFrame,
    features: list[str],
) -> pd.DataFrame:
    """
    Predicts anomalies and calculates scores and severity.

    Returns:
        DataFrame copy with 'is_anomaly', 'anomaly_score', and 'severity'.
    """
    df = data.copy()
    X = df[features].fillna(0.0)

    # isolation forest: -1 is anomaly, 1 is normal
    raw_preds = model.predict(X)
    scores = model.decision_function(X)

    df["is_anomaly"] = raw_preds == -1
    df["anomaly_score"] = scores

    z_scores = df["z_score_power"] if "z_score_power" in df.columns else np.zeros(len(df))

    severities = []
    for score, z in zip(scores, z_scores):
        severities.append(classify_severity(score, float(z)))
    df["severity"] = severities

    return df


def save_model(model: IsolationForest, filepath: str) -> None:
    """Saves model to disk using joblib."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    joblib.dump(model, filepath)
    logger.info("Model successfully saved to %s", filepath)


def load_model(filepath: str) -> IsolationForest:
    """Loads trained model from disk."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Model file not found: {filepath}")
    model = joblib.load(filepath)
    logger.info("Model loaded from %s", filepath)
    return model

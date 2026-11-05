"""
MLflow Evaluation and Tracking for Energy Forecasting & Anomalies.
Sprint 6B - ML Engine
Logs metrics (RMSE, MAE, MAPE) and registers models in MLflow.
"""

import logging
import os
from typing import Any

import numpy as np

logger = logging.getLogger("evaluate_mlflow")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def calculate_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """
    Computes regression evaluation metrics:
    - MAE: Mean Absolute Error
    - RMSE: Root Mean Squared Error
    - MAPE: Mean Absolute Percentage Error (%)
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    if len(y_true) == 0 or len(y_pred) == 0:
        return {"mae": 0.0, "rmse": 0.0, "mape": 0.0}

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    # Avoid zero division in MAPE
    non_zero = y_true != 0
    if np.any(non_zero):
        mape = float(np.mean(np.abs((y_true[non_zero] - y_pred[non_zero]) / y_true[non_zero])) * 100.0)
    else:
        mape = 0.0

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "mape": round(mape, 2),
    }


def validate_forecasting_sla(mape: float, threshold_pct: float = 10.0) -> tuple[bool, str]:
    """
    Validates if model satisfies contractual SLA (MAPE < 10%).
    """
    passed = mape <= threshold_pct
    status = f"PASSED: MAPE={mape:.2f}% <= {threshold_pct}%" if passed else f"FAILED: MAPE={mape:.2f}% > {threshold_pct}%"
    return passed, status


def log_experiment_to_mlflow(
    experiment_name: str,
    run_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    model: Any | None = None,
    artifact_path: str | None = None,
) -> dict[str, Any]:
    """
    Logs run parameters, metrics, and models to MLflow.
    Gracefully handles environment where MLflow server is offline/mocked.
    """
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "databricks")
    logger.info("Attempting MLflow logging to: %s (experiment: %s)", tracking_uri, experiment_name)

    result = {
        "status": "logged_locally",
        "experiment": experiment_name,
        "run_name": run_name,
        "params": params,
        "metrics": metrics,
    }

    try:
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=run_name) as run:
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)

            if model is not None and hasattr(mlflow, "sklearn"):
                try:
                    mlflow.sklearn.log_model(model, artifact_path="model")
                except Exception as ex:
                    logger.warning("Could not log model artifact to MLflow: %s", ex)

            if artifact_path and os.path.exists(artifact_path):
                mlflow.log_artifact(artifact_path)

            result["status"] = "logged_mlflow"
            result["run_id"] = run.info.run_id
            logger.info("MLflow run logged successfully. Run ID: %s", run.info.run_id)

    except Exception as e:
        logger.warning("MLflow remote tracking unavailable (%s). Metrics kept locally: %s", e, metrics)

    return result

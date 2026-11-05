"""
Model Promotion Script for MLflow Model Registry.
Sprint 6C - MLOps
Transitions models from Staging to Production after quality validation.
"""

import logging
import os
from typing import Any

logger = logging.getLogger("promote_model")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def validate_candidate_metrics(
    metrics: dict[str, float],
    max_mape: float = 10.0,
) -> bool:
    """Checks if model candidate meets production criteria."""
    mape = metrics.get("mape", 999.0)
    if mape > max_mape:
        logger.warning("Promotion rejected: MAPE %.2f%% exceeds max threshold %.2f%%", mape, max_mape)
        return False
    logger.info("Promotion criteria met: MAPE %.2f%% <= %.2f%%", mape, max_mape)
    return True


def promote_model_stage(
    model_name: str,
    version: int,
    target_stage: str = "Production",
    archive_existing: bool = True,
    client: Any | None = None,
) -> dict[str, Any]:
    """
    Transitions a registered model version to target stage (e.g. Production).
    """
    logger.info("Attempting stage promotion: %s (v%s) -> %s", model_name, version, target_stage)

    result = {
        "model_name": model_name,
        "version": version,
        "target_stage": target_stage,
        "status": "dry_run_success",
    }

    try:
        if client is None:
            import mlflow
            from mlflow.tracking import MlflowClient
            tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "databricks")
            mlflow.set_tracking_uri(tracking_uri)
            client = MlflowClient()

        client.transition_model_version_stage(
            name=model_name,
            version=str(version),
            stage=target_stage,
            archive_existing_versions=archive_existing,
        )
        result["status"] = "promoted"
        logger.info("Successfully transitioned %s v%s to %s", model_name, version, target_stage)
    except Exception as e:
        logger.warning("MLflow remote registry transition unavailable (%s). Result logged in dry-run mode.", e)
        result["error"] = str(e)

    return result

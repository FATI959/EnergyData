"""
Tests for ML Engine (Sprint 6).
Covers Anomaly Detection, Forecasting, MLflow Tracking, and Model Registry.
"""

import os
import tempfile
import uuid

import numpy as np
import pandas as pd
import pytest

from ml_engine.anomaly_detection.stream_scoring import score_micro_batch
from ml_engine.anomaly_detection.train_detector import (
    classify_severity,
    determine_anomaly_type,
    load_model,
    predict_anomalies,
    save_model,
    train_isolation_forest,
)
from ml_engine.forecasting.evaluate_mlflow import (
    calculate_metrics,
    log_experiment_to_mlflow,
    validate_forecasting_sla,
)
from ml_engine.forecasting.train_forecaster import EnergyLoadForecaster
from ml_engine.model_registry.promote_model import (
    promote_model_stage,
    validate_candidate_metrics,
)
from ml_engine.model_registry.rollback_model import rollback_model_version

# =============================================================================
# 1. Anomaly Detection Unit Tests
# =============================================================================

class TestAnomalyDetection:
    @pytest.fixture
    def sample_telemetry(self):
        np.random.seed(42)
        n = 100
        return pd.DataFrame(
            {
                "voltage_v": np.random.normal(230.0, 5.0, n),
                "current_a": np.random.normal(25.0, 3.0, n),
                "active_power_kw": np.random.normal(5.75, 0.7, n),
                "power_factor": np.random.normal(0.95, 0.02, n),
                "frequency_hz": np.random.normal(50.0, 0.1, n),
                "z_score_power": np.random.normal(0.0, 1.0, n),
                "device_id": [f"DEV_{i % 5:02d}" for i in range(n)],
                "timestamp": pd.date_range("2026-11-05", periods=n, freq="5min"),
            }
        )

    def test_classify_severity_levels(self):
        assert classify_severity(anomaly_score=-0.3, z_score=4.5) == "Critical"
        assert classify_severity(anomaly_score=-0.26, z_score=1.0) == "Critical"
        assert classify_severity(anomaly_score=-0.15, z_score=3.2) == "Medium"
        assert classify_severity(anomaly_score=-0.05, z_score=1.2) == "Low"

    def test_determine_anomaly_type(self):
        assert determine_anomaly_type(voltage=190.0) == "voltage_drop"
        assert determine_anomaly_type(voltage=260.0) == "voltage_spike"
        assert determine_anomaly_type(frequency=48.9) == "frequency_drift"
        assert determine_anomaly_type(power_factor=0.70) == "power_factor_degradation"
        assert determine_anomaly_type(z_score=3.5) == "load_surge"
        assert determine_anomaly_type(z_score=-3.5) == "power_drop"
        assert determine_anomaly_type() == "general_anomaly"

    def test_train_and_predict_isolation_forest(self, sample_telemetry):
        model, features = train_isolation_forest(sample_telemetry, contamination=0.05)
        assert model is not None
        assert len(features) == 5

        scored = predict_anomalies(model, sample_telemetry, features)
        assert "is_anomaly" in scored.columns
        assert "anomaly_score" in scored.columns
        assert "severity" in scored.columns
        assert scored["severity"].isin(["Low", "Medium", "Critical"]).all()

    def test_score_micro_batch_output_schema(self, sample_telemetry):
        model, features = train_isolation_forest(sample_telemetry, contamination=0.10)
        # Inject one obvious outlier
        sample_telemetry.loc[0, "voltage_v"] = 100.0
        sample_telemetry.loc[0, "active_power_kw"] = 500.0

        anomalies_df = score_micro_batch(sample_telemetry, model, features)
        required_cols = {
            "anomaly_id",
            "device_id",
            "timestamp",
            "anomaly_type",
            "severity",
            "z_score",
        }
        assert required_cols.issubset(set(anomalies_df.columns))
        if not anomalies_df.empty:
            # Check UUID format
            first_id = anomalies_df["anomaly_id"].iloc[0]
            assert uuid.UUID(first_id)

    def test_model_persistence(self, sample_telemetry):
        model, _ = train_isolation_forest(sample_telemetry)
        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
            temp_path = f.name
        try:
            save_model(model, temp_path)
            assert os.path.exists(temp_path)
            loaded = load_model(temp_path)
            assert loaded is not None
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


# =============================================================================
# 2. Forecasting Unit Tests
# =============================================================================

class TestLoadForecasting:
    @pytest.fixture
    def timeseries_data(self):
        np.random.seed(42)
        n = 150
        dates = pd.date_range("2026-11-01", periods=n, freq="5min")
        return pd.DataFrame(
            {
                "window_start": dates,
                "hour_of_day": dates.hour,
                "day_of_week": dates.dayofweek,
                "is_peak_hour": (dates.hour >= 8) & (dates.hour < 20),
                "rolling_avg_power_3w": np.random.uniform(20.0, 45.0, n),
                "lag_1_power": np.random.uniform(20.0, 45.0, n),
                "lag_3_power": np.random.uniform(20.0, 45.0, n),
                "avg_active_power_kw": np.random.uniform(20.0, 50.0, n),
                "substation_id": "SUB_NORTH_01",
            }
        )

    def test_forecaster_fit_and_predict(self, timeseries_data):
        forecaster = EnergyLoadForecaster(model_version="v1.0.0")
        forecaster.fit(timeseries_data)

        mean_p, lower_p, upper_p = forecaster.predict(timeseries_data)
        assert len(mean_p) == len(timeseries_data)
        assert (mean_p >= 0.0).all()
        # Confidence bound property: lower <= mean <= upper
        assert (lower_p <= mean_p + 1e-5).all()
        assert (mean_p <= upper_p + 1e-5).all()

    def test_generate_forecast_dataframe_contract(self, timeseries_data):
        forecaster = EnergyLoadForecaster()
        forecaster.fit(timeseries_data)

        forecast_df = forecaster.generate_forecast_dataframe(
            future_features_df=timeseries_data.iloc[:12],
            substation_id="SUB_NORTH_01",
        )
        expected_columns = {
            "forecast_id",
            "substation_id",
            "target_timestamp",
            "predicted_active_power_kw",
            "lower_bound_kw",
            "upper_bound_kw",
            "model_version",
        }
        assert expected_columns == set(forecast_df.columns)
        assert len(forecast_df) == 12
        assert forecast_df["predicted_active_power_kw"].dtype in [np.float64, float]

    def test_forecaster_save_and_load(self, timeseries_data):
        forecaster = EnergyLoadForecaster()
        forecaster.fit(timeseries_data)

        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
            temp_path = f.name
        try:
            forecaster.save(temp_path)
            loaded = EnergyLoadForecaster.load(temp_path)
            preds, _, _ = loaded.predict(timeseries_data.iloc[:5])
            assert len(preds) == 5
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


# =============================================================================
# 3. MLflow Evaluation & Metrics Tests
# =============================================================================

class TestMLflowEvaluation:
    def test_metrics_calculation(self):
        y_true = np.array([100.0, 110.0, 95.0, 105.0])
        y_pred = np.array([102.0, 108.0, 97.0, 103.0])

        metrics = calculate_metrics(y_true, y_pred)
        assert "mae" in metrics
        assert "rmse" in metrics
        assert "mape" in metrics
        assert metrics["mae"] == 2.0
        assert metrics["mape"] < 5.0

    def test_forecasting_sla_validation(self):
        passed, msg = validate_forecasting_sla(mape=6.5, threshold_pct=10.0)
        assert passed is True
        assert "PASSED" in msg

        failed, msg_f = validate_forecasting_sla(mape=12.4, threshold_pct=10.0)
        assert failed is False
        assert "FAILED" in msg_f

    def test_log_experiment_fallback(self):
        res = log_experiment_to_mlflow(
            experiment_name="energy_forecasting_test",
            run_name="test_run",
            params={"n_estimators": 100},
            metrics={"mape": 5.2, "rmse": 3.1},
        )
        assert res["status"] in ["logged_mlflow", "logged_locally"]
        assert res["metrics"]["mape"] == 5.2


# =============================================================================
# 4. Model Registry & MLOps Tests
# =============================================================================

class TestModelRegistry:
    def test_validate_candidate_metrics(self):
        assert validate_candidate_metrics({"mape": 8.5}, max_mape=10.0) is True
        assert validate_candidate_metrics({"mape": 14.2}, max_mape=10.0) is False

    def test_promote_model_stage_safe_execution(self):
        res = promote_model_stage(
            model_name="IsolationForest_Detector",
            version=1,
            target_stage="Production",
        )
        assert res["model_name"] == "IsolationForest_Detector"
        assert res["target_stage"] == "Production"

    def test_rollback_model_version_safe_execution(self):
        res = rollback_model_version(
            model_name="IsolationForest_Detector",
            target_stable_version=1,
        )
        assert res["restored_version"] == 1

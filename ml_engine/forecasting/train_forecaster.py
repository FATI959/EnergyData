"""
Load Forecasting Model Training & Inference (H+24 / J+7).
Sprint 6B - ML Engine
Outputs conform to 'gold_load_forecasts' Delta table contract.
"""

import logging
import os
import uuid
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor

logger = logging.getLogger("train_forecaster")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

FORECAST_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "is_peak_hour",
    "rolling_avg_power_3w",
    "lag_1_power",
    "lag_3_power",
]


class EnergyLoadForecaster:
    """
    Forecaster for substation electrical load with prediction intervals.
    Uses XGBoost if available, with robust fallback to GradientBoosting/RandomForest.
    """

    def __init__(
        self,
        model_type: str = "gradient_boosting",
        model_version: str = "v1.0.0",
        random_state: int = 42,
    ):
        self.model_type = model_type
        self.model_version = model_version
        self.random_state = random_state
        self.model_mean = None
        self.model_lower = None
        self.model_upper = None
        self.feature_names: list[str] = []

    def _init_models(self):
        try:
            import xgboost as xgb
            has_xgb = True
        except ImportError:
            has_xgb = False

        if self.model_type == "xgboost" and has_xgb:
            self.model_mean = xgb.XGBRegressor(
                n_estimators=100,
                learning_rate=0.08,
                max_depth=5,
                random_state=self.random_state,
            )
        else:
            self.model_mean = RandomForestRegressor(
                n_estimators=100,
                max_depth=6,
                random_state=self.random_state,
            )

        # Quantile regressors for confidence intervals (10th and 90th percentiles)
        self.model_lower = GradientBoostingRegressor(
            loss="quantile",
            alpha=0.10,
            n_estimators=70,
            max_depth=4,
            random_state=self.random_state,
        )
        self.model_upper = GradientBoostingRegressor(
            loss="quantile",
            alpha=0.90,
            n_estimators=70,
            max_depth=4,
            random_state=self.random_state,
        )

    def fit(
        self,
        df: pd.DataFrame,
        target_col: str = "avg_active_power_kw",
        feature_cols: list[str] | None = None,
    ) -> "EnergyLoadForecaster":
        """Fits mean and confidence bound regressors."""
        self._init_models()

        if feature_cols is None:
            feature_cols = [c for c in FORECAST_FEATURES if c in df.columns]
            if not feature_cols:
                # Basic default features
                feature_cols = [c for c in ["hour_of_day", "day_of_week"] if c in df.columns]
                if not feature_cols:
                    feature_cols = [target_col]

        self.feature_names = feature_cols
        X = df[self.feature_names].fillna(0.0)
        y = df[target_col].fillna(0.0)

        logger.info("Fitting load forecaster with %d samples and %d features", len(X), len(feature_cols))
        self.model_mean.fit(X, y)
        self.model_lower.fit(X, y)
        self.model_upper.fit(X, y)
        logger.info("Forecaster fit successfully.")
        return self

    def predict(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Returns predictions (mean, lower_bound, upper_bound).
        """
        X = df[self.feature_names].fillna(0.0)
        mean_preds = self.model_mean.predict(X)
        lower_preds = self.model_lower.predict(X)
        upper_preds = self.model_upper.predict(X)

        # Enforce physical constraints: load >= 0 and lower <= mean <= upper
        lower_preds = np.maximum(0.0, np.minimum(lower_preds, mean_preds))
        upper_preds = np.maximum(mean_preds, upper_preds)
        mean_preds = np.maximum(0.0, mean_preds)

        return mean_preds, lower_preds, upper_preds

    def generate_forecast_dataframe(
        self,
        future_features_df: pd.DataFrame,
        substation_id: str = "SUB_01",
        start_timestamp: pd.Timestamp | None = None,
        freq_minutes: int = 5,
    ) -> pd.DataFrame:
        """
        Generates forecast records compliant with 'gold_load_forecasts' Delta table schema.

        Columns:
        - forecast_id (UUID string)
        - substation_id (string)
        - target_timestamp (Timestamp)
        - predicted_active_power_kw (double)
        - lower_bound_kw (double)
        - upper_bound_kw (double)
        - model_version (string)
        """
        mean_p, lower_p, upper_p = self.predict(future_features_df)
        n = len(future_features_df)

        if start_timestamp is None:
            start_timestamp = pd.Timestamp.now()

        timestamps = [
            start_timestamp + pd.Timedelta(minutes=i * freq_minutes)
            for i in range(n)
        ]

        sub_id = future_features_df["substation_id"].iloc[0] if (
            "substation_id" in future_features_df.columns and not future_features_df.empty
        ) else substation_id

        records: list[dict[str, Any]] = []
        for i in range(n):
            records.append(
                {
                    "forecast_id": str(uuid.uuid4()),
                    "substation_id": str(sub_id),
                    "target_timestamp": timestamps[i],
                    "predicted_active_power_kw": float(round(mean_p[i], 4)),
                    "lower_bound_kw": float(round(lower_p[i], 4)),
                    "upper_bound_kw": float(round(upper_p[i], 4)),
                    "model_version": self.model_version,
                }
            )

        return pd.DataFrame(records)

    def save(self, filepath: str) -> None:
        """Saves forecaster object."""
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        joblib.dump(self, filepath)
        logger.info("Forecaster saved to %s", filepath)

    @staticmethod
    def load(filepath: str) -> "EnergyLoadForecaster":
        """Loads forecaster object."""
        forecaster = joblib.load(filepath)
        logger.info("Forecaster loaded from %s", filepath)
        return forecaster

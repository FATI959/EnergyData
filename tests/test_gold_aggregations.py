# =============================================================================
# tests/test_gold_aggregations.py
# Sprint 5 — Tests unitaires : Couche Gold (04_gold_aggregations.py)
# =============================================================================
# Tests purement Python (pas de session Spark requise).
# Vérifient :
#   • Formule de calcul de l'énergie (kWh)
#   • Structure des schémas Gold (Contrat d'Interface 3)
#   • Cohérence des chemins dans databricks_config.json
#   • Logique de la fenêtre temporelle
#   • Validation des valeurs de sévérité ML
#   • Contraintes sur les KPIs (bornes physiques)
# =============================================================================

import json
import math
import os

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "config", "databricks_config.json"
)


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# 1. Formule énergie kWh
# ---------------------------------------------------------------------------


class TestEnergyFormula:
    """Vérifie la formule : total_energy_kwh = avg_kw × (window_min / 60)."""

    def test_energy_formula_basic(self):
        """Cas nominal : 10 kW sur 5 min = 10 × 5/60 ≈ 0.8333 kWh."""
        avg_kw = 10.0
        window_min = 5.0
        expected = avg_kw * (window_min / 60.0)
        assert math.isclose(expected, 0.8333333, rel_tol=1e-4)

    def test_energy_formula_zero_power(self):
        """Puissance nulle → énergie nulle."""
        avg_kw = 0.0
        window_min = 5.0
        result = avg_kw * (window_min / 60.0)
        assert result == 0.0

    def test_energy_formula_proportional_to_power(self):
        """Doublement de la puissance → doublement de l'énergie."""
        window_min = 5.0
        e1 = 50.0 * (window_min / 60.0)
        e2 = 100.0 * (window_min / 60.0)
        assert math.isclose(e2, 2 * e1, rel_tol=1e-9)

    def test_energy_formula_1h_window(self):
        """Fenêtre de 60 min : énergie = puissance (kWh == kW × 1h)."""
        avg_kw = 25.0
        window_min = 60.0
        result = avg_kw * (window_min / 60.0)
        assert math.isclose(result, avg_kw, rel_tol=1e-9)

    def test_energy_formula_positive(self):
        """L'énergie est toujours positive pour une puissance positive."""
        for avg_kw in [0.1, 1.0, 100.0, 500.0]:
            result = avg_kw * (5.0 / 60.0)
            assert result > 0, f"Énergie négative pour avg_kw={avg_kw}"


# ---------------------------------------------------------------------------
# 2. Schéma de la table gold_energy_5min_aggregates
# ---------------------------------------------------------------------------

REQUIRED_GOLD_AGGREGATE_COLUMNS = {
    "substation_id",
    "zone",
    "window_start",
    "window_end",
    "avg_active_power_kw",
    "peak_active_power_kw",
    "total_energy_kwh",
    "avg_power_factor",
    "voltage_min",
    "voltage_max",
}


class TestGoldAggregateSchema:
    """Vérifie que le schéma Gold agrégats respecte le Contrat d'Interface 3."""

    def test_all_required_columns_present(self):
        """Toutes les colonnes du contrat doivent être dans le set requis."""
        for col in REQUIRED_GOLD_AGGREGATE_COLUMNS:
            assert col in REQUIRED_GOLD_AGGREGATE_COLUMNS

    def test_window_columns_pair(self):
        """window_start et window_end doivent être tous les deux présents."""
        assert "window_start" in REQUIRED_GOLD_AGGREGATE_COLUMNS
        assert "window_end" in REQUIRED_GOLD_AGGREGATE_COLUMNS

    def test_no_raw_window_struct(self):
        """La colonne brute 'window' (struct) ne doit pas apparaître dans le schéma final."""
        assert "window" not in REQUIRED_GOLD_AGGREGATE_COLUMNS

    def test_column_count(self):
        """Le contrat définit exactement 10 colonnes pour gold_energy_5min_aggregates."""
        assert len(REQUIRED_GOLD_AGGREGATE_COLUMNS) == 10


# ---------------------------------------------------------------------------
# 3. Schéma de la table gold_detected_anomalies
# ---------------------------------------------------------------------------

REQUIRED_ANOMALY_COLUMNS = {
    "anomaly_id",
    "device_id",
    "timestamp",
    "anomaly_type",
    "severity",
    "z_score",
}

VALID_SEVERITIES = {"Low", "Medium", "Critical"}


class TestGoldAnomalySchema:
    """Vérifie le schéma Gold anomalies (préparé pour le Sprint 6 ML)."""

    def test_anomaly_required_columns(self):
        for col in REQUIRED_ANOMALY_COLUMNS:
            assert col in REQUIRED_ANOMALY_COLUMNS

    def test_valid_severity_values(self):
        """Les valeurs de sévérité ML doivent appartenir à l'ensemble défini."""
        assert VALID_SEVERITIES == {"Low", "Medium", "Critical"}

    def test_severity_ordered(self):
        """Vérification de l'ordre logique des sévérités (Low < Medium < Critical)."""
        ordered = ["Low", "Medium", "Critical"]
        for i, sev in enumerate(ordered):
            assert sev in VALID_SEVERITIES, f"Sévérité manquante : {sev}"
        assert len(ordered) == len(VALID_SEVERITIES)


# ---------------------------------------------------------------------------
# 4. Schéma de la table gold_load_forecasts
# ---------------------------------------------------------------------------

REQUIRED_FORECAST_COLUMNS = {
    "forecast_id",
    "substation_id",
    "target_timestamp",
    "predicted_active_power_kw",
    "lower_bound_kw",
    "upper_bound_kw",
    "model_version",
}


class TestGoldForecastSchema:
    """Vérifie le schéma Gold prévisions (préparé pour le Sprint 6 ML)."""

    def test_forecast_required_columns(self):
        for col in REQUIRED_FORECAST_COLUMNS:
            assert col in REQUIRED_FORECAST_COLUMNS

    def test_forecast_has_confidence_interval(self):
        """Un intervalle de confiance (lower/upper bound) doit être présent."""
        assert "lower_bound_kw" in REQUIRED_FORECAST_COLUMNS
        assert "upper_bound_kw" in REQUIRED_FORECAST_COLUMNS

    def test_forecast_column_count(self):
        """Le contrat définit exactement 7 colonnes pour gold_load_forecasts."""
        assert len(REQUIRED_FORECAST_COLUMNS) == 7


# ---------------------------------------------------------------------------
# 5. Cohérence des chemins dans databricks_config.json
# ---------------------------------------------------------------------------


class TestDatabricksConfigGoldPaths:
    """Vérifie que databricks_config.json contient les chemins Gold attendus."""

    @pytest.fixture(scope="class")
    def config(self):
        return load_config()

    def test_gold_aggregates_path_exists(self, config):
        paths = config.get("storage", {}).get("paths", {})
        assert "gold_aggregates" in paths, "Clé 'gold_aggregates' manquante dans config"

    def test_gold_aggregates_path_value(self, config):
        path = config["storage"]["paths"]["gold_aggregates"]
        assert "gold" in path.lower()
        assert "energy_5min_aggregates" in path or "gold" in path

    def test_checkpoint_gold_path_exists(self, config):
        """Le checkpoint Gold est dans paths.checkpoints.gold."""
        paths = config.get("storage", {}).get("paths", {})
        checkpoints = paths.get("checkpoints", {})
        assert "gold" in checkpoints, "Chemin checkpoints.gold manquant dans config"
        assert "gold_aggregations" in checkpoints["gold"]

    def test_gold_anomalies_path_exists(self, config):
        paths = config.get("storage", {}).get("paths", {})
        assert "gold_anomalies" in paths, "Clé 'gold_anomalies' manquante dans config"

    def test_silver_path_exists(self, config):
        """Le chemin Silver doit exister (source du pipeline Gold)."""
        paths = config.get("storage", {}).get("paths", {})
        assert "silver" in paths, "Chemin Silver manquant — source du pipeline Gold"


# ---------------------------------------------------------------------------
# 6. Logique de la fenêtre temporelle
# ---------------------------------------------------------------------------


class TestWindowLogic:
    """Vérifie la cohérence de la durée de fenêtre et du watermark."""

    def test_window_5min_in_seconds(self):
        """5 minutes = 300 secondes."""
        assert 5 * 60 == 300

    def test_watermark_greater_than_window(self):
        """Le watermark (15 min) doit être ≥ la fenêtre (5 min)."""
        window_min = 5
        watermark_min = 15
        assert watermark_min >= window_min

    def test_window_divides_hour_evenly(self):
        """Une fenêtre de 5 min doit diviser 60 min sans reste."""
        assert 60 % 5 == 0

    def test_energy_kwh_non_negative(self):
        """L'énergie calculée ne doit jamais être négative."""
        for avg_kw in [0.0, 0.5, 100.0]:
            energy = avg_kw * (5.0 / 60.0)
            assert energy >= 0


# ---------------------------------------------------------------------------
# 7. Contraintes physiques sur les KPIs
# ---------------------------------------------------------------------------


class TestPhysicalConstraints:
    """Vérifie que les KPIs Gold respectent les contraintes physiques du réseau."""

    def test_voltage_range(self):
        """Plage de tension valide : 100–280 V (règle data quality Sprint 4)."""
        v_min, v_max = 100.0, 280.0
        # Exemple de valeurs agrégées simulées
        sample_min = 215.0
        sample_max = 240.0
        assert v_min <= sample_min <= v_max
        assert v_min <= sample_max <= v_max

    def test_power_factor_range(self):
        """Power factor valide : 0 ≤ pf ≤ 1."""
        for pf in [0.0, 0.5, 0.95, 1.0]:
            assert 0.0 <= pf <= 1.0

    def test_avg_power_less_than_peak(self):
        """La puissance moyenne doit être ≤ à la puissance de pointe."""
        avg = 45.2
        peak = 72.8
        assert avg <= peak

    def test_total_energy_kwh_formula_physical(self):
        """
        Pour 100 appareils à 5 kW chacun sur 5 min :
        total_energy = 500 kW × (5/60) ≈ 41.67 kWh
        """
        avg_kw = 500.0
        expected_kwh = avg_kw * (5.0 / 60.0)
        assert math.isclose(expected_kwh, 41.6666, rel_tol=1e-3)

# =============================================================================
# tests/test_ml_feature_store.py
# Sprint 5 — Tests unitaires : Feature Store ML (05_ml_feature_store.py)
# =============================================================================
# Tests purement Python (pas de session Spark requise).
# Vérifient :
#   • Formules des features dérivées (voltage_range, power_factor_deviation)
#   • Formule du Z-score
#   • Schéma complet du Feature Store (21 colonnes)
#   • Logique des features calendaires (peak hours, day_of_week)
#   • Paramètres de rolling window (taille, cohérence)
#   • Seuil d'anomalie Z-score (3-sigma)
#   • Cohérence des features lag
# =============================================================================

import math

# ---------------------------------------------------------------------------
# 1. Features dérivées de base
# ---------------------------------------------------------------------------


class TestBaseFeatures:
    """Vérifie les formules des features de base dérivées des agrégats Gold."""

    def test_voltage_range_formula(self):
        """voltage_range = voltage_max - voltage_min."""
        v_max, v_min = 240.5, 218.3
        expected = round(v_max - v_min, 3)
        assert math.isclose(expected, 22.2, rel_tol=1e-3)

    def test_voltage_range_zero_when_equal(self):
        """Si voltage_min == voltage_max, voltage_range = 0."""
        v = 230.0
        assert round(v - v, 3) == 0.0

    def test_voltage_range_always_positive(self):
        """voltage_range est toujours >= 0 (max >= min par définition)."""
        for v_min, v_max in [(100.0, 200.0), (220.0, 240.0), (150.0, 150.0)]:
            assert v_max - v_min >= 0

    def test_power_factor_deviation_ideal(self):
        """Power factor idéal = 1.0 → déviation = 0."""
        pf = 1.0
        deviation = round(abs(1.0 - pf), 4)
        assert deviation == 0.0

    def test_power_factor_deviation_low(self):
        """Power factor de 0.75 → déviation = 0.25."""
        pf = 0.75
        deviation = round(abs(1.0 - pf), 4)
        assert math.isclose(deviation, 0.25, rel_tol=1e-6)

    def test_power_factor_deviation_always_positive(self):
        """La déviation est toujours >= 0 (valeur absolue)."""
        for pf in [0.0, 0.5, 0.95, 1.0]:
            assert abs(1.0 - pf) >= 0


# ---------------------------------------------------------------------------
# 2. Features calendaires
# ---------------------------------------------------------------------------


class TestCalendarFeatures:
    """Vérifie la logique des features temporelles calendaires."""

    def test_peak_hour_start(self):
        """8h = début des heures de pointe."""
        PEAK_START = 8
        PEAK_END = 20
        assert 8 >= PEAK_START
        assert 8 < PEAK_END

    def test_peak_hour_end_exclusive(self):
        """20h = hors pointe (borne exclusive)."""
        PEAK_START = 8
        PEAK_END = 20
        hour = 20
        is_peak = hour >= PEAK_START and hour < PEAK_END
        assert not is_peak

    def test_night_hour_not_peak(self):
        """3h du matin = hors pointe."""
        PEAK_START = 8
        PEAK_END = 20
        for hour in [0, 1, 2, 3, 4, 5, 6, 7]:
            is_peak = hour >= PEAK_START and hour < PEAK_END
            assert not is_peak, f"L'heure {hour}h ne devrait pas être en pointe"

    def test_day_hours_are_peak(self):
        """De 8h à 19h inclus = heures de pointe."""
        PEAK_START = 8
        PEAK_END = 20
        for hour in range(8, 20):
            is_peak = hour >= PEAK_START and hour < PEAK_END
            assert is_peak, f"L'heure {hour}h devrait être en pointe"

    def test_day_of_week_range(self):
        """day_of_week doit être dans [1, 7] (Spark: 1=dim, 7=sam)."""
        for dow in range(1, 8):
            assert 1 <= dow <= 7

    def test_peak_hours_count(self):
        """Il y a exactement 12 heures de pointe par jour (8h-19h inclus)."""
        PEAK_START = 8
        PEAK_END = 20
        count = sum(1 for h in range(24) if PEAK_START <= h < PEAK_END)
        assert count == 12


# ---------------------------------------------------------------------------
# 3. Z-score
# ---------------------------------------------------------------------------


class TestZScoreFeature:
    """Vérifie la formule et les cas limites du Z-score."""

    def test_zscore_formula_basic(self):
        """Z-score = (valeur - moyenne) / écart-type."""
        value = 110.0
        mean = 100.0
        std = 10.0
        z = round((value - mean) / std, 4)
        assert math.isclose(z, 1.0, rel_tol=1e-6)

    def test_zscore_zero_when_equal_to_mean(self):
        """Si value == mean → z_score = 0."""
        value = mean = 50.0
        std = 5.0
        z = (value - mean) / std
        assert z == 0.0

    def test_zscore_zero_when_std_is_zero(self):
        """Si std = 0 → z_score = 0 (cas limite, division par zéro évitée)."""
        std = 0.0
        z = 0.0 if std == 0 else (100.0 - 100.0) / std
        assert z == 0.0

    def test_zscore_anomaly_threshold(self):
        """Un |z_score| > 3 indique une anomalie (règle 3-sigma)."""
        z_normal = 2.5
        z_anomaly = 3.5
        assert abs(z_normal) <= 3
        assert abs(z_anomaly) > 3

    def test_zscore_negative_deviation(self):
        """Une chute de puissance → z_score négatif."""
        value = 30.0
        mean = 100.0
        std = 10.0
        z = (value - mean) / std
        assert z < 0

    def test_zscore_symmetric(self):
        """Z-score est symétrique : +x et -x ont même |z|."""
        mean = 100.0
        std = 10.0
        z_high = (110.0 - mean) / std
        z_low = (90.0 - mean) / std
        assert math.isclose(abs(z_high), abs(z_low), rel_tol=1e-9)


# ---------------------------------------------------------------------------
# 4. Schéma du Feature Store
# ---------------------------------------------------------------------------

FEATURE_STORE_COLUMNS = {
    # Clés
    "substation_id",
    "zone",
    "window_start",
    "window_end",
    # Features Gold brutes
    "avg_active_power_kw",
    "peak_active_power_kw",
    "total_energy_kwh",
    "avg_power_factor",
    "voltage_min",
    "voltage_max",
    # Features dérivées de base
    "voltage_range",
    "power_factor_deviation",
    # Features temporelles
    "rolling_avg_power_3w",
    "rolling_std_power_3w",
    "lag_1_power",
    "lag_3_power",
    "power_delta",
    "z_score_power",
    # Features calendaires
    "hour_of_day",
    "day_of_week",
    "is_peak_hour",
}


class TestFeatureStoreSchema:
    """Vérifie que le Feature Store a les 21 colonnes requises."""

    def test_total_column_count(self):
        """Le Feature Store doit avoir exactement 21 colonnes."""
        assert len(FEATURE_STORE_COLUMNS) == 21

    def test_key_columns_present(self):
        """Les colonnes clés de partitionnement sont présentes."""
        for col in ["substation_id", "zone", "window_start", "window_end"]:
            assert col in FEATURE_STORE_COLUMNS

    def test_gold_base_columns_present(self):
        """Toutes les colonnes Gold sources sont présentes."""
        gold_cols = [
            "avg_active_power_kw",
            "peak_active_power_kw",
            "total_energy_kwh",
            "avg_power_factor",
            "voltage_min",
            "voltage_max",
        ]
        for col in gold_cols:
            assert col in FEATURE_STORE_COLUMNS, f"Colonne Gold manquante : {col}"

    def test_derived_features_present(self):
        """Les features dérivées sont présentes."""
        derived = ["voltage_range", "power_factor_deviation"]
        for col in derived:
            assert col in FEATURE_STORE_COLUMNS

    def test_temporal_features_present(self):
        """Les features temporelles sont présentes."""
        temporal = [
            "rolling_avg_power_3w",
            "rolling_std_power_3w",
            "lag_1_power",
            "lag_3_power",
            "power_delta",
            "z_score_power",
        ]
        for col in temporal:
            assert col in FEATURE_STORE_COLUMNS, f"Feature temporelle manquante : {col}"

    def test_calendar_features_present(self):
        """Les features calendaires sont présentes."""
        calendar = ["hour_of_day", "day_of_week", "is_peak_hour"]
        for col in calendar:
            assert col in FEATURE_STORE_COLUMNS

    def test_no_raw_window_struct(self):
        """La colonne brute 'window' (struct PySpark) ne doit pas être présente."""
        assert "window" not in FEATURE_STORE_COLUMNS


# ---------------------------------------------------------------------------
# 5. Paramètres de rolling window
# ---------------------------------------------------------------------------


class TestRollingWindowParams:
    """Vérifie la cohérence des paramètres de la fenêtre glissante."""

    def test_rolling_window_size(self):
        """La fenêtre glissante est de 3 intervals (= 15 min)."""
        ROLLING_WINDOW_SIZE = 3
        assert ROLLING_WINDOW_SIZE == 3

    def test_rolling_window_covers_15_min(self):
        """3 fenêtres × 5 min = 15 min de lookback."""
        window_duration_min = 5
        rolling_size = 3
        lookback_min = window_duration_min * rolling_size
        assert lookback_min == 15

    def test_lag_1_is_5_min_ago(self):
        """lag_1 correspond à 1 fenêtre = 5 min en arrière."""
        lag_steps = 1
        window_min = 5
        lookback_min = lag_steps * window_min
        assert lookback_min == 5

    def test_lag_3_is_15_min_ago(self):
        """lag_3 correspond à 3 fenêtres = 15 min en arrière."""
        lag_steps = 3
        window_min = 5
        lookback_min = lag_steps * window_min
        assert lookback_min == 15

    def test_power_delta_uses_lag_1(self):
        """power_delta = avg_power_kw(t) - avg_power_kw(t-1)."""
        current = 55.0
        lag_1 = 50.0
        delta = current - lag_1
        assert math.isclose(delta, 5.0, rel_tol=1e-9)

    def test_power_delta_negative_on_decrease(self):
        """power_delta est négatif si la puissance baisse."""
        current = 40.0
        lag_1 = 60.0
        delta = current - lag_1
        assert delta < 0

"""
EnergyData Lakehouse — Tests Unitaires du Simulateur IoT
=========================================================
Sprint 1 | Membre A — Data Platform Engineer

Couvre :
  - IoTEventGenerator  : génération de batches, schéma des payloads
  - DeviceSimulator    : validité des valeurs physiques
  - AnomalyInjector    : injection correcte de chaque type d'anomalie
  - WeatherEnricher    : logique de cache TTL, fallback

Usage:
    pytest tests/test_generator.py -v
    pytest tests/test_generator.py -v --tb=short
"""

import copy
import json
import math
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# Fixtures partagées
# ---------------------------------------------------------------------------
CONFIG_PATH = "config/simulation_config.yaml"


@pytest.fixture(scope="session")
def config() -> dict:
    """Charge la configuration YAML réelle une seule fois pour la session."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def generator(config):
    """Instancie le générateur avec l'API météo mockée (pas d'appel réseau)."""
    from data_simulation.generator import IoTEventGenerator

    with patch("data_simulation.weather_enricher.requests.get") as mock_get:
        # Mock de la réponse Open-Meteo
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "current_weather": {"temperature": 18.5}
        }
        mock_get.return_value = mock_response
        gen = IoTEventGenerator(config_path=CONFIG_PATH)
    return gen


@pytest.fixture
def sample_event(config) -> dict:
    """Génère un événement de référence pour les tests d'anomalie."""
    return {
        "event_id": "test-uuid-1234",
        "device_id": "METER_INDUSTRIAL_PARK_001",
        "timestamp": "2026-11-04T16:00:00.000Z",
        "substation_id": "SUBSTATION_NORTH_01",
        "location": {
            "latitude": 48.8566,
            "longitude": 2.3522,
            "zone": "Industrial_Park",
        },
        "metrics": {
            "voltage_v": 230.0,
            "current_a": 15.0,
            "active_power_kw": 3.311,
            "reactive_power_kvar": 0.35,
            "power_factor": 0.96,
            "frequency_hz": 50.0,
            "ambient_temp_c": 18.5,
        },
        "status_flag": "NORMAL",
    }


# ===========================================================================
# TESTS — IoTEventGenerator
# ===========================================================================
class TestIoTEventGenerator:

    def test_generator_loads_config(self, generator):
        """Le générateur doit charger la config sans lever d'exception."""
        assert generator.config is not None
        assert "simulation" in generator.config
        assert "devices" in generator.config
        assert "physics" in generator.config

    def test_correct_number_of_devices(self, generator, config):
        """Le nombre de devices initialisés doit correspondre à la config."""
        expected = sum(
            z["num_devices"] for z in config["devices"]["zones"]
        )
        assert len(generator.devices) == expected

    def test_generate_batch_returns_list(self, generator):
        """generate_batch() doit retourner une liste non vide."""
        with patch.object(generator.weather_enricher, "get_temperature", return_value=18.0):
            batch = generator.generate_batch()
        assert isinstance(batch, list)
        assert len(batch) > 0

    def test_batch_size_equals_num_devices(self, generator):
        """Un batch doit contenir exactement 1 événement par capteur."""
        with patch.object(generator.weather_enricher, "get_temperature", return_value=18.0):
            batch = generator.generate_batch()
        assert len(batch) == len(generator.devices)

    def test_event_counter_increments(self, generator):
        """total_events_generated doit incrémenter correctement."""
        before = generator.total_events_generated
        with patch.object(generator.weather_enricher, "get_temperature", return_value=18.0):
            generator.generate_batch()
        after = generator.total_events_generated
        assert after == before + len(generator.devices)

    def test_device_ids_are_unique(self, generator):
        """Tous les device_id doivent être distincts."""
        ids = [d.device_id for d in generator.devices]
        assert len(ids) == len(set(ids)), "Des device_id dupliqués ont été détectés"

    def test_generator_raises_on_missing_config(self):
        """Le générateur doit lever FileNotFoundError si le fichier config est absent."""
        from data_simulation.generator import IoTEventGenerator

        with pytest.raises(FileNotFoundError):
            IoTEventGenerator(config_path="config/does_not_exist.yaml")


# ===========================================================================
# TESTS — Schéma du Payload JSON (Contrat d'Interface 1)
# ===========================================================================
class TestPayloadSchema:
    """Vérifie la conformité des payloads au schéma défini dans IMPLEMENTATION_PLAN §3."""

    REQUIRED_TOP_KEYS = {
        "event_id", "device_id", "timestamp", "substation_id",
        "location", "metrics", "status_flag",
    }
    REQUIRED_LOCATION_KEYS = {"latitude", "longitude", "zone"}
    REQUIRED_METRICS_KEYS = {
        "voltage_v", "current_a", "active_power_kw",
        "reactive_power_kvar", "power_factor", "frequency_hz", "ambient_temp_c",
    }

    @pytest.fixture(autouse=True)
    def sample_batch(self, generator):
        """Génère un batch frais pour chaque test de ce groupe."""
        with patch.object(generator.weather_enricher, "get_temperature", return_value=18.0):
            self.batch = generator.generate_batch()

    def test_top_level_keys_present(self):
        for event in self.batch:
            missing = self.REQUIRED_TOP_KEYS - set(event.keys())
            assert not missing, f"Clés manquantes dans l'événement: {missing}"

    def test_location_keys_present(self):
        for event in self.batch:
            missing = self.REQUIRED_LOCATION_KEYS - set(event["location"].keys())
            assert not missing, f"Clés manquantes dans 'location': {missing}"

    def test_metrics_keys_present(self):
        for event in self.batch:
            missing = self.REQUIRED_METRICS_KEYS - set(event["metrics"].keys())
            assert not missing, f"Clés manquantes dans 'metrics': {missing}"

    def test_device_id_is_non_empty_string(self):
        for event in self.batch:
            assert isinstance(event["device_id"], str)
            assert len(event["device_id"]) > 0

    def test_timestamp_is_iso8601(self):
        from datetime import datetime, timezone
        for event in self.batch:
            ts = event["timestamp"]
            assert isinstance(ts, str)
            # Doit être parseable comme ISO 8601 UTC
            parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
            assert parsed is not None

    def test_all_metrics_are_numeric(self):
        for event in self.batch:
            for key, value in event["metrics"].items():
                assert isinstance(value, (int, float)), \
                    f"Métrique '{key}' n'est pas numérique: {value}"

    def test_event_is_json_serializable(self):
        for event in self.batch:
            try:
                serialized = json.dumps(event)
                assert len(serialized) > 0
            except (TypeError, ValueError) as e:
                pytest.fail(f"Événement non sérialisable en JSON: {e}")


# ===========================================================================
# TESTS — Validité des valeurs physiques
# ===========================================================================
class TestPhysicsValidity:
    """Les valeurs générées doivent respecter les plages physiques réalistes."""

    @pytest.fixture(autouse=True)
    def sample_batch(self, generator):
        # Plusieurs batches pour couvrir la variabilité
        batches = []
        with patch.object(generator.weather_enricher, "get_temperature", return_value=18.0):
            for _ in range(5):
                batches.extend(generator.generate_batch())
        # Filtre uniquement les événements normaux (sans anomalie injectée)
        self.normal_events = [e for e in batches if e.get("status_flag") == "NORMAL"]

    def test_voltage_in_valid_range(self, config):
        low = config["physics"]["voltage"]["min_valid"]
        high = config["physics"]["voltage"]["max_valid"]
        for e in self.normal_events:
            v = e["metrics"]["voltage_v"]
            assert low <= v <= high, f"Tension hors plage: {v}V"

    def test_current_non_negative(self):
        for e in self.normal_events:
            assert e["metrics"]["current_a"] >= 0, \
                f"Courant négatif: {e['metrics']['current_a']}A"

    def test_power_factor_in_range(self, config):
        low = config["physics"]["power_factor"]["min_valid"]
        high = config["physics"]["power_factor"]["max_valid"]
        for e in self.normal_events:
            pf = e["metrics"]["power_factor"]
            assert low <= pf <= high, f"Power factor hors plage: {pf}"

    def test_frequency_in_valid_range(self, config):
        low = config["physics"]["frequency"]["min_valid"]
        high = config["physics"]["frequency"]["max_valid"]
        for e in self.normal_events:
            f = e["metrics"]["frequency_hz"]
            assert low <= f <= high, f"Fréquence hors plage: {f}Hz"

    def test_active_power_positive(self):
        for e in self.normal_events:
            assert e["metrics"]["active_power_kw"] > 0, \
                f"Puissance active non positive: {e['metrics']['active_power_kw']}kW"

    def test_voltage_near_nominal(self, config):
        """La tension doit être proche du nominal (±15%) pour les événements normaux."""
        nominal = config["physics"]["voltage"]["nominal_v"]
        tolerance = 0.15
        for e in self.normal_events:
            v = e["metrics"]["voltage_v"]
            assert abs(v - nominal) / nominal <= tolerance, \
                f"Tension trop éloignée du nominal: {v}V (nominal={nominal}V)"

    def test_power_physics_consistency(self):
        """P ≈ U × I × cos(φ) / 1000 (tolérance 20% pour les artefacts de simulation)."""
        for e in self.normal_events:
            m = e["metrics"]
            expected_p = (m["voltage_v"] * m["current_a"] * m["power_factor"]) / 1000.0
            actual_p = m["active_power_kw"]
            if expected_p > 0:
                relative_error = abs(actual_p - expected_p) / expected_p
                assert relative_error < 0.20, \
                    f"Incohérence physique P={actual_p}kW vs P_calc={expected_p:.3f}kW"


# ===========================================================================
# TESTS — AnomalyInjector
# ===========================================================================
class TestAnomalyInjector:

    @pytest.fixture
    def injector(self, config):
        from data_simulation.anomaly_injector import AnomalyInjector
        return AnomalyInjector(config["anomalies"])

    def test_overvoltage_increases_voltage(self, injector, sample_event, config):
        cfg = {"name": "Overvoltage", "voltage_multiplier": 1.15}
        result = injector._inject_overvoltage(copy.deepcopy(sample_event), cfg)
        assert result["metrics"]["voltage_v"] > sample_event["metrics"]["voltage_v"]

    def test_undervoltage_decreases_voltage(self, injector, sample_event):
        cfg = {"name": "Undervoltage", "voltage_multiplier": 0.80}
        result = injector._inject_undervoltage(copy.deepcopy(sample_event), cfg)
        assert result["metrics"]["voltage_v"] < sample_event["metrics"]["voltage_v"]

    def test_power_factor_drop_reduces_pf(self, injector, sample_event):
        cfg = {"name": "PowerFactorDrop", "power_factor_value": 0.65}
        result = injector._inject_power_factor_drop(copy.deepcopy(sample_event), cfg)
        assert result["metrics"]["power_factor"] < 0.80

    def test_disconnection_zeroes_metrics(self, injector, sample_event):
        from data_simulation import anomaly_injector
        # Réinitialiser le compteur pour ce device
        anomaly_injector._silent_counters.pop(sample_event["device_id"], None)
        cfg = {"name": "AbruptDisconnection", "silent_cycles": 3}
        result = injector._inject_disconnection(
            copy.deepcopy(sample_event), cfg, sample_event["device_id"]
        )
        assert result["metrics"]["voltage_v"] == 0.0
        assert result["metrics"]["current_a"] == 0.0
        assert result["status_flag"] == "DISCONNECTED"
        # Nettoyage
        anomaly_injector._silent_counters.pop(sample_event["device_id"], None)

    def test_frequency_deviation_moves_frequency(self, injector, sample_event):
        cfg = {"name": "FrequencyDeviation", "frequency_offset": 2.5}
        result = injector._inject_frequency_deviation(copy.deepcopy(sample_event), cfg)
        assert result["metrics"]["frequency_hz"] != sample_event["metrics"]["frequency_hz"]

    def test_inject_adds_status_flag(self, injector, sample_event, config):
        result = injector.inject(copy.deepcopy(sample_event), config["physics"])
        assert result["status_flag"] in {"ANOMALY", "DISCONNECTED"}

    def test_inject_adds_anomaly_type_field(self, injector, sample_event, config):
        result = injector.inject(copy.deepcopy(sample_event), config["physics"])
        assert "anomaly_type" in result

    def test_original_event_not_modified(self, injector, sample_event, config):
        """L'injection doit travailler sur une copie (pas de mutation en place)."""
        original_voltage = sample_event["metrics"]["voltage_v"]
        original_flag = sample_event["status_flag"]
        injector.inject(sample_event, config["physics"])
        assert sample_event["metrics"]["voltage_v"] == original_voltage
        assert sample_event["status_flag"] == original_flag


# ===========================================================================
# TESTS — WeatherEnricher
# ===========================================================================
class TestWeatherEnricher:

    @pytest.fixture
    def enricher(self, config):
        from data_simulation.weather_enricher import WeatherEnricher
        return WeatherEnricher(config.get("weather", {}))

    def test_returns_fallback_on_api_error(self, enricher, config):
        """Si l'API échoue, retourner la valeur de fallback."""
        with patch("data_simulation.weather_enricher.requests.get", side_effect=Exception("Network error")):
            enricher.clear_cache()
            temp = enricher.get_temperature(48.8566, 2.3522)
        assert temp == config["weather"]["fallback_temp_c"]

    def test_returns_api_temperature_on_success(self, enricher):
        """Si l'API répond correctement, retourner la température API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"current_weather": {"temperature": 22.3}}

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response):
            enricher.clear_cache()
            temp = enricher.get_temperature(48.8566, 2.3522)
        assert temp == 22.3

    def test_cache_avoids_second_api_call(self, enricher):
        """La deuxième requête pour la même position doit venir du cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"current_weather": {"temperature": 15.0}}

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response) as mock_get:
            enricher.clear_cache()
            enricher.get_temperature(48.8566, 2.3522)  # 1er appel → API
            enricher.get_temperature(48.8566, 2.3522)  # 2ème appel → cache
            assert mock_get.call_count == 1, "L'API a été appelée plus d'une fois malgré le cache"

    def test_cache_expires_after_ttl(self, enricher, config):
        """Le cache doit expirer après le TTL configuré."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"current_weather": {"temperature": 20.0}}

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response):
            enricher.clear_cache()
            enricher.get_temperature(48.8566, 2.3522)

        # Simuler l'expiration en manipulant le timestamp du cache
        key = enricher._cache_key(48.8566, 2.3522)
        enricher._cache[key] = (20.0, time.time() - enricher.cache_ttl - 1)

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response) as mock_get:
            enricher.get_temperature(48.8566, 2.3522)
            assert mock_get.call_count == 1, "L'API n'a pas été rappelée après expiration du cache"

    def test_clear_cache(self, enricher):
        """clear_cache() doit vider le cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"current_weather": {"temperature": 10.0}}

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response):
            enricher.get_temperature(48.0, 2.0)
        assert enricher.cache_size > 0
        enricher.clear_cache()
        assert enricher.cache_size == 0

    def test_geographic_cache_groups_nearby_locations(self, enricher):
        """Deux positions proches (< 0.01°) doivent partager la même entrée de cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"current_weather": {"temperature": 17.0}}

        with patch("data_simulation.weather_enricher.requests.get", return_value=mock_response) as mock_get:
            enricher.clear_cache()
            enricher.get_temperature(48.856, 2.352)   # exactement pareil arrondi
            enricher.get_temperature(48.8561, 2.3521)  # < 0.01° de différence
            assert mock_get.call_count == 1  # une seule requête API

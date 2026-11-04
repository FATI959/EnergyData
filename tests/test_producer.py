"""
EnergyData Lakehouse — Tests Unitaires du Producteur Azure Event Hubs
=====================================================================
Sprint 2 | Membre A — Data Platform Engineer

Valide la logique métier du producteur sans dépendance réseau Azure réelle :
  - Métriques de performance (débit, comptage d'erreurs, temps écoulé)
  - Validation et gestion des credentials manquants
  - Sérialisation et packaging en EventDataBatch
  - Algorithme de retry exponentiel sur défaillance réseau
  - Épuisement des retries et bascule en erreur
  - Exécution du pipeline en mode dry-run et streaming simulé

Usage:
    pytest tests/test_producer.py -v
"""

import os
import time
from unittest.mock import MagicMock, patch

import pytest

from data_simulation.producer_eventhubs import (
    EventHubsProducer,
    ProducerMetrics,
    run_pipeline,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_events() -> list[dict]:
    """Événements de test conformes au format télémétrique."""
    return [
        {
            "event_id": "test-uuid-001",
            "device_id": "METER_INDUSTRIAL_PARK_001",
            "timestamp": "2026-11-04T17:00:00.000Z",
            "substation_id": "SUBSTATION_NORTH_01",
            "location": {"latitude": 48.8566, "longitude": 2.3522, "zone": "Industrial_Park"},
            "metrics": {
                "voltage_v": 230.5,
                "current_a": 18.2,
                "active_power_kw": 4.195,
                "reactive_power_kvar": 1.32,
                "power_factor": 0.954,
                "frequency_hz": 50.01,
                "ambient_temp_c": 18.0,
            },
            "status_flag": "NORMAL",
        },
        {
            "event_id": "test-uuid-002",
            "device_id": "METER_RESIDENTIAL_002",
            "timestamp": "2026-11-04T17:00:00.000Z",
            "substation_id": "SUBSTATION_SOUTH_02",
            "location": {"latitude": 48.8300, "longitude": 2.3700, "zone": "Residential"},
            "metrics": {
                "voltage_v": 229.8,
                "current_a": 12.1,
                "active_power_kw": 2.75,
                "reactive_power_kvar": 0.85,
                "power_factor": 0.955,
                "frequency_hz": 49.99,
                "ambient_temp_c": 18.0,
            },
            "status_flag": "NORMAL",
        },
    ]


@pytest.fixture
def mock_eventhub_client():
    """Mock complet du client EventHubProducerClient et de son batch."""
    client = MagicMock()
    batch = MagicMock()
    client.create_batch.return_value = batch
    client.__enter__.return_value = client
    client.__exit__.return_value = None
    return client, batch


# ===========================================================================
# 1. Tests des métriques du producteur
# ===========================================================================
class TestProducerMetrics:

    def test_metrics_initial_state(self):
        metrics = ProducerMetrics()
        assert metrics.total_sent == 0
        assert metrics.total_errors == 0
        assert metrics.total_batches == 0
        assert metrics.elapsed_seconds >= 0.0

    def test_record_batch_increments_correctly(self):
        metrics = ProducerMetrics()
        metrics.record_batch(50)
        metrics.record_batch(25)
        assert metrics.total_sent == 75
        assert metrics.total_batches == 2

    def test_record_error_increments(self):
        metrics = ProducerMetrics()
        metrics.record_error()
        metrics.record_error()
        assert metrics.total_errors == 2

    def test_messages_per_second_calculation(self):
        metrics = ProducerMetrics()
        metrics.start_time = time.time() - 2.0  # simuler 2 secondes écoulées
        metrics.record_batch(100)
        rate = metrics.messages_per_second
        assert 45.0 <= rate <= 55.0  # ~50 msg/s

    def test_report_executes_without_exception(self):
        metrics = ProducerMetrics()
        metrics.record_batch(10)
        # Ne doit pas lever d'exception
        metrics.report()


# ===========================================================================
# 2. Tests de validation & initialisation du producteur
# ===========================================================================
class TestEventHubsProducerInit:

    def test_missing_connection_string_raises_value_error(self, monkeypatch):
        monkeypatch.delenv("EVENT_HUBS_CONNECTION_STRING", raising=False)
        monkeypatch.delenv("EVENT_HUB_NAME", raising=False)
        with pytest.raises(ValueError, match="EVENT_HUBS_CONNECTION_STRING manquante"):
            EventHubsProducer(connection_string=None, eventhub_name="test-hub")

    def test_missing_hub_name_raises_value_error(self, monkeypatch):
        monkeypatch.delenv("EVENT_HUBS_CONNECTION_STRING", raising=False)
        monkeypatch.delenv("EVENT_HUB_NAME", raising=False)
        with pytest.raises(ValueError, match="EVENT_HUB_NAME manquante"):
            EventHubsProducer(
                connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
                eventhub_name=None,
            )

    def test_explicit_credentials_accepted(self):
        producer = EventHubsProducer(
            connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
            eventhub_name="energy-test-stream",
        )
        assert producer.connection_string.startswith("Endpoint=sb://")
        assert producer.eventhub_name == "energy-test-stream"
        assert producer.metrics.total_sent == 0


# ===========================================================================
# 3. Tests d'envoi en batch et tolérance aux pannes
# ===========================================================================
class TestEventHubsProducerSendBatch:

    def test_send_batch_success(self, sample_events, mock_eventhub_client):
        mock_client, mock_batch = mock_eventhub_client

        producer = EventHubsProducer(
            connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
            eventhub_name="energy-test-stream",
        )

        with patch.object(producer, "_get_client", return_value=mock_client):
            success = producer.send_batch(sample_events)

        assert success is True
        assert producer.metrics.total_sent == len(sample_events)
        assert producer.metrics.total_batches == 1
        assert producer.metrics.total_errors == 0
        assert mock_batch.add.call_count == len(sample_events)
        mock_client.send_batch.assert_called_once_with(mock_batch)

    def test_retry_on_transient_failure_then_success(
        self, sample_events, mock_eventhub_client
    ):
        mock_client, mock_batch = mock_eventhub_client
        # 1er appel : échec réseau (ex: socket timeout)
        # 2ème appel : succès
        mock_client.send_batch.side_effect = [
            ConnectionResetError("Transient network drop"),
            None,
        ]

        producer = EventHubsProducer(
            connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
            eventhub_name="energy-test-stream",
        )
        producer.BASE_RETRY_DELAY = 0.01  # Accélérer le test unitaire

        with patch.object(producer, "_get_client", return_value=mock_client):
            success = producer.send_batch(sample_events)

        assert success is True
        assert mock_client.send_batch.call_count == 2
        assert producer.metrics.total_sent == len(sample_events)
        assert producer.metrics.total_errors == 0

    def test_retry_exhaustion_returns_false_and_records_error(
        self, sample_events, mock_eventhub_client
    ):
        mock_client, _ = mock_eventhub_client
        # 3 échecs consécutifs -> abandon
        mock_client.send_batch.side_effect = TimeoutError("Azure Gateway Timeout")

        producer = EventHubsProducer(
            connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
            eventhub_name="energy-test-stream",
        )
        producer.BASE_RETRY_DELAY = 0.01

        with patch.object(producer, "_get_client", return_value=mock_client):
            success = producer.send_batch(sample_events)

        assert success is False
        assert mock_client.send_batch.call_count == producer.MAX_RETRIES
        assert producer.metrics.total_sent == 0
        assert producer.metrics.total_errors == 1

    def test_close_handles_open_client(self):
        producer = EventHubsProducer(
            connection_string="Endpoint=sb://fake.servicebus.windows.net/;",
            eventhub_name="energy-test-stream",
        )
        mock_client = MagicMock()
        producer._client = mock_client
        producer.close()
        mock_client.close.assert_called_once()
        assert producer._client is None


# ===========================================================================
# 4. Tests du Pipeline complet (Run Pipeline)
# ===========================================================================
class TestRunPipeline:

    def test_run_pipeline_dry_run_executes_without_azure(self, capsys):
        # En mode dry-run, aucune connexion Azure ne doit être tentée
        run_pipeline(
            config_path="config/simulation_config.yaml",
            dry_run=True,
        )
        captured = capsys.readouterr()
        # Le dry run doit afficher du JSON formaté
        assert "METER_" in captured.out

    def test_run_pipeline_streaming_batches(self, sample_events):
        with patch(
            "data_simulation.producer_eventhubs.EventHubsProducer"
        ) as MockProducerClass, patch("time.sleep") as mock_sleep:
            mock_producer_instance = MagicMock()
            mock_producer_instance.send_batch.return_value = True
            mock_producer_instance.metrics.total_sent = 100
            mock_producer_instance.metrics.elapsed_seconds = 0.5
            MockProducerClass.return_value = mock_producer_instance

            # Génération de 2 batches seulement
            run_pipeline(
                config_path="config/simulation_config.yaml",
                num_batches=2,
                dry_run=False,
            )

            assert mock_producer_instance.send_batch.call_count == 2
            mock_producer_instance.close.assert_called_once()

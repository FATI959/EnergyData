"""
EnergyData Lakehouse — Azure Event Hubs Producer
=================================================
Sprint 2 | Membre A — Data Platform Engineer

Client d'envoi streaming vers Azure Event Hubs via le protocole AMQP.
Intègre le générateur IoT (Sprint 1) pour un pipeline complet :
  Simulation → Sérialisation JSON → Batching → Event Hubs

Fonctionnalités:
  - Envoi asynchrone en batch (EventDataBatch) pour maximiser le débit
  - Retry exponentiel en cas d'erreur transitoire (3 tentatives)
  - Métriques de performance : messages/sec, latence d'envoi, erreurs
  - Mode 'dry-run' pour tester le générateur sans Azure

Usage:
    # Depuis l'environnement virtuel activé
    python -m data_simulation.producer_eventhubs
    python -m data_simulation.producer_eventhubs --rate 100
    python -m data_simulation.producer_eventhubs --dry-run
    python -m data_simulation.producer_eventhubs --num-batches 10

Prérequis:
    Renseigner dans .env (copié depuis .env.example) :
        EVENT_HUBS_CONNECTION_STRING="Endpoint=sb://..."
        EVENT_HUB_NAME="energy-telemetry-stream"
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement depuis .env
load_dotenv()


# ---------------------------------------------------------------------------
# Métriques de performance
# ---------------------------------------------------------------------------
class ProducerMetrics:
    """Collecte et affiche les métriques du producteur Event Hubs."""

    def __init__(self):
        self.total_sent = 0
        self.total_errors = 0
        self.total_batches = 0
        self.start_time = time.time()
        self._last_report_time = time.time()
        self._last_report_count = 0

    def record_batch(self, count: int) -> None:
        self.total_sent += count
        self.total_batches += 1

    def record_error(self) -> None:
        self.total_errors += 1

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def messages_per_second(self) -> float:
        elapsed = self.elapsed_seconds
        return self.total_sent / elapsed if elapsed > 0 else 0.0

    def report(self) -> None:
        """Affiche un rapport périodique des métriques."""
        now = time.time()
        interval = now - self._last_report_time
        interval_msgs = self.total_sent - self._last_report_count

        if interval > 0:
            rate = interval_msgs / interval
        else:
            rate = 0.0

        logger.info(
            "📊 Métriques — Total: %d msgs | Batches: %d | "
            "Débit: %.1f msg/s | Erreurs: %d | Durée: %.0fs",
            self.total_sent,
            self.total_batches,
            rate,
            self.total_errors,
            self.elapsed_seconds,
        )
        self._last_report_time = now
        self._last_report_count = self.total_sent


# ---------------------------------------------------------------------------
# EventHubsProducer
# ---------------------------------------------------------------------------
class EventHubsProducer:
    """
    Producteur Azure Event Hubs pour les événements IoT.
    
    Encapsule le client azure-eventhub et gère :
    - La connexion sécurisée via connection string
    - L'envoi en batch (EventDataBatch) pour maximiser le débit
    - Le retry exponentiel en cas d'erreur transitoire
    """

    MAX_RETRIES = 3
    BASE_RETRY_DELAY = 1.0  # secondes

    def __init__(
        self,
        connection_string: Optional[str] = None,
        eventhub_name: Optional[str] = None,
    ):
        """
        Args:
            connection_string: Chaîne de connexion Event Hubs.
                               Si None, lue depuis EVENT_HUBS_CONNECTION_STRING.
            eventhub_name: Nom du hub.
                           Si None, lu depuis EVENT_HUB_NAME.

        Raises:
            ValueError: Si les credentials sont manquants.
            ImportError: Si azure-eventhub n'est pas installé.
        """
        self.connection_string = connection_string or os.getenv(
            "EVENT_HUBS_CONNECTION_STRING"
        )
        self.eventhub_name = eventhub_name or os.getenv("EVENT_HUB_NAME")

        if not self.connection_string:
            raise ValueError(
                "EVENT_HUBS_CONNECTION_STRING manquante. "
                "Copiez .env.example → .env et renseignez la valeur."
            )
        if not self.eventhub_name:
            raise ValueError(
                "EVENT_HUB_NAME manquante. "
                "Copiez .env.example → .env et renseignez la valeur."
            )

        # Import conditionnel pour permettre les tests sans Azure SDK
        try:
            from azure.eventhub import EventHubProducerClient, EventData
            self._EventHubProducerClient = EventHubProducerClient
            self._EventData = EventData
        except ImportError as e:
            raise ImportError(
                "azure-eventhub non installé. Lancez : pip install azure-eventhub"
            ) from e

        self._client: Optional[object] = None
        self.metrics = ProducerMetrics()
        logger.info(
            "EventHubsProducer configured — Hub: %s", self.eventhub_name
        )

    def _get_client(self):
        """Crée (ou réutilise) le client Event Hubs."""
        if self._client is None:
            self._client = self._EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name,
            )
            logger.info("Event Hubs client connected.")
        return self._client

    def send_batch(self, events: list[dict]) -> bool:
        """
        Envoie un batch d'événements vers Event Hubs.

        Args:
            events: Liste de payloads JSON à envoyer.

        Returns:
            bool: True si l'envoi a réussi, False sinon.
        """
        client = self._get_client()
        attempt = 0
        while attempt < self.MAX_RETRIES:
            try:
                t_start = time.time()
                with client:
                    # Création du batch Event Hubs (taille auto-gérée par l'SDK)
                    event_data_batch = client.create_batch()
                    for event in events:
                        payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
                        event_data_batch.add(self._EventData(payload))

                    client.send_batch(event_data_batch)

                latency_ms = (time.time() - t_start) * 1000
                self.metrics.record_batch(len(events))
                logger.debug(
                    "Batch sent — %d events | latency: %.1fms", len(events), latency_ms
                )
                # Recréer le client pour le prochain appel (pattern recommandé)
                self._client = None
                return True

            except Exception as e:
                attempt += 1
                self._client = None  # Forcer reconnexion
                if attempt < self.MAX_RETRIES:
                    delay = self.BASE_RETRY_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        "Envoi échoué (tentative %d/%d) : %s — Retry dans %.1fs",
                        attempt, self.MAX_RETRIES, e, delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Envoi définitivement échoué après %d tentatives : %s",
                        self.MAX_RETRIES, e,
                    )
                    self.metrics.record_error()
                    return False
        return False

    def close(self) -> None:
        """Ferme proprement la connexion Event Hubs."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            logger.info("Event Hubs client closed.")


# ---------------------------------------------------------------------------
# Pipeline complet : Generateur → Event Hubs
# ---------------------------------------------------------------------------
def run_pipeline(
    config_path: str = "config/simulation_config.yaml",
    num_batches: Optional[int] = None,
    target_rate: Optional[int] = None,
    report_interval: int = 30,
    dry_run: bool = False,
) -> None:
    """
    Lance le pipeline complet : IoTEventGenerator → EventHubsProducer.

    Args:
        config_path:     Chemin vers simulation_config.yaml
        num_batches:     Nombre de batches (None = infini)
        target_rate:     Débit cible en messages/sec (None = vitesse config)
        report_interval: Intervalle de reporting des métriques (secondes)
        dry_run:         Si True, génère sans envoyer vers Event Hubs
    """
    from data_simulation.generator import IoTEventGenerator

    generator = IoTEventGenerator(config_path=config_path)
    num_devices = len(generator.devices)

    if dry_run:
        logger.info("=== MODE DRY-RUN : aucun envoi vers Event Hubs ===")
        batch = generator.generate_batch()
        logger.info("Batch de %d événements générés :", len(batch))
        for event in batch[:5]:
            print(json.dumps(event, indent=2, ensure_ascii=False))
        logger.info("... (%d autres événements non affichés)", max(0, len(batch) - 5))
        return

    producer = EventHubsProducer()
    logger.info(
        "Pipeline démarré — %d devices | Hub: %s | target_rate: %s msg/s",
        num_devices,
        producer.eventhub_name,
        target_rate or "auto (config)",
    )

    # Calcul de l'intervalle d'envoi pour respecter le débit cible
    if target_rate:
        interval = num_devices / target_rate
    else:
        interval = generator.config["simulation"]["emit_interval_seconds"]

    last_report = time.time()
    batch_num = 0

    try:
        while num_batches is None or batch_num < num_batches:
            t0 = time.time()

            # Génération du batch
            events = generator.generate_batch()

            # Envoi vers Event Hubs
            success = producer.send_batch(events)
            if not success:
                logger.warning("Batch %d non envoyé, continuation...", batch_num)

            batch_num += 1

            # Rapport périodique
            if time.time() - last_report >= report_interval:
                producer.metrics.report()
                last_report = time.time()

            # Respect de l'intervalle
            elapsed = time.time() - t0
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Pipeline arrêté par l'utilisateur.")

    finally:
        producer.close()
        producer.metrics.report()
        logger.info(
            "Pipeline terminé — Total envoyé: %d msgs en %.0fs",
            producer.metrics.total_sent,
            producer.metrics.elapsed_seconds,
        )


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="EnergyData — Azure Event Hubs Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python -m data_simulation.producer_eventhubs --dry-run
  python -m data_simulation.producer_eventhubs --rate 500
  python -m data_simulation.producer_eventhubs --num-batches 20
        """,
    )
    parser.add_argument(
        "--config",
        default="config/simulation_config.yaml",
        help="Chemin vers simulation_config.yaml",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Débit cible en messages/sec (ex: 500)",
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=None,
        help="Nombre de batches à envoyer (défaut: infini)",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=30,
        help="Intervalle de rapport métriques en secondes (défaut: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Génère et affiche un batch sans l'envoyer vers Event Hubs",
    )
    args = parser.parse_args()

    run_pipeline(
        config_path=args.config,
        num_batches=args.num_batches,
        target_rate=args.rate,
        report_interval=args.report_interval,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()


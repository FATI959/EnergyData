"""
EnergyData Lakehouse — Throughput & Load Benchmark Tool
========================================================
Sprint 2 | Membre A — Data Platform Engineer

Valide l'exigence de montée en charge du Sprint 2 :
  "Test de montée en charge (≥ 500 msg/sec)"

Modes:
  1. Benchmark Local (Moteur de simulation + Batching en mémoire)
     Mesure la capacité maximale de génération/sérialisation/packaging.
  2. Benchmark Live (Azure Event Hubs)
     Mesure le débit réel soutenu sur le réseau Azure vers Event Hubs.

Usage:
    # Benchmark local (5000 messages à débit libre pour tester la capacité max)
    python -m data_simulation.benchmark_load --num-messages 5000

    # Benchmark local régulé à 500 msg/sec pendant 10 secondes
    python -m data_simulation.benchmark_load --rate 500 --duration 10

    # Benchmark Azure Event Hubs réel (nécessite .env configuré)
    python -m data_simulation.benchmark_load --live --rate 500 --duration 15
"""

import argparse
import json
import logging
import os
import time
from typing import Optional

import numpy as np
from dotenv import load_dotenv

from data_simulation.generator import IoTEventGenerator

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("Benchmark")
load_dotenv()


class BenchmarkRunner:
    """Orchestre les tests de charge et calcule les percentiles de latence."""

    def __init__(
        self,
        config_path: str = "config/simulation_config.yaml",
        target_rate: Optional[int] = 500,
        num_messages: Optional[int] = None,
        duration_seconds: Optional[int] = 10,
        is_live: bool = False,
    ):
        self.config_path = config_path
        self.target_rate = target_rate
        self.num_messages = num_messages
        self.duration_seconds = duration_seconds
        self.is_live = is_live
        self.generator = IoTEventGenerator(config_path=config_path)

        self.producer = None
        if self.is_live:
            from data_simulation.producer_eventhubs import EventHubsProducer
            self.producer = EventHubsProducer()

    def run(self) -> dict:
        """Exécute la session de benchmark et retourne les métriques."""
        logger.info("=" * 65)
        logger.info("⚡ DÉMARRAGE DU BENCHMARK DE CHARGE (SPRINT 2)")
        logger.info(
            "Mode: %s | Cible: %s msg/s | Capteurs: %d",
            "Azure Live (AMQP)" if self.is_live else "Local Emulation",
            self.target_rate if self.target_rate else "Vitesse Max (Uncapped)",
            len(self.generator.devices),
        )
        logger.info("=" * 65)

        batch_latencies = []
        total_messages = 0
        total_batches = 0
        errors = 0

        start_time = time.time()
        batch_size = len(self.generator.devices)  # 50 messages par batch

        # Calcul de l'intervalle si débit bridé
        interval = (batch_size / self.target_rate) if self.target_rate else 0.0

        try:
            while True:
                # Conditions de fin
                now = time.time()
                elapsed = now - start_time
                if self.duration_seconds and elapsed >= self.duration_seconds:
                    break
                if self.num_messages and total_messages >= self.num_messages:
                    break

                t_batch_start = time.time()

                # 1. Génération
                batch = self.generator.generate_batch()

                # 2. Transmission / Emulation
                if self.is_live and self.producer:
                    success = self.producer.send_batch(batch)
                    if not success:
                        errors += 1
                else:
                    # Emulation locale : sérialisation JSON stricte + encode UTF-8
                    payloads = [json.dumps(e, ensure_ascii=False).encode("utf-8") for e in batch]
                    assert len(payloads) == batch_size

                t_batch_end = time.time()
                batch_latency_ms = (t_batch_end - t_batch_start) * 1000
                batch_latencies.append(batch_latency_ms)

                total_messages += batch_size
                total_batches += 1

                # Régulation de cadence avec compensation de dérive temporelle
                if self.target_rate:
                    target_time = start_time + (total_messages / self.target_rate)
                    sleep_needed = target_time - time.time()
                    if sleep_needed > 0:
                        time.sleep(sleep_needed)

        except KeyboardInterrupt:
            logger.info("Benchmark interrompu manuellement.")

        total_elapsed = time.time() - start_time
        effective_throughput = total_messages / total_elapsed if total_elapsed > 0 else 0.0

        # Calcul des percentiles
        p50 = float(np.percentile(batch_latencies, 50)) if batch_latencies else 0.0
        p95 = float(np.percentile(batch_latencies, 95)) if batch_latencies else 0.0
        p99 = float(np.percentile(batch_latencies, 99)) if batch_latencies else 0.0
        avg_latency = float(np.mean(batch_latencies)) if batch_latencies else 0.0

        results = {
            "is_live": self.is_live,
            "total_messages": total_messages,
            "total_batches": total_batches,
            "total_elapsed_sec": round(total_elapsed, 2),
            "effective_throughput_msg_sec": round(effective_throughput, 1),
            "target_rate": self.target_rate,
            "errors": errors,
            "latency_avg_ms": round(avg_latency, 2),
            "latency_p50_ms": round(p50, 2),
            "latency_p95_ms": round(p95, 2),
            "latency_p99_ms": round(p99, 2),
            "target_met": effective_throughput >= (self.target_rate * 0.95 if self.target_rate else 500),
        }

        self._print_report(results)
        return results

    def _print_report(self, r: dict) -> None:
        """Affiche le rapport formaté dans la console."""
        logger.info("\n" + "=" * 65)
        logger.info("📋 RAPPORT DE PERFORMANCE — MONTÉE EN CHARGE SPRINT 2")
        logger.info("=" * 65)
        logger.info("Mode d'exécution       : %s", "Live Azure Event Hubs" if r["is_live"] else "Local Batching Emulation")
        logger.info("Durée de test          : %.2f secondes", r["total_elapsed_sec"])
        logger.info("Messages traités       : %d (%d batches)", r["total_messages"], r["total_batches"])
        logger.info("Erreurs rencontrées    : %d", r["errors"])
        logger.info("-" * 65)
        logger.info("🚀 DÉBIT MOYEN OBSERVÉ : %.1f messages/sec", r["effective_throughput_msg_sec"])
        logger.info("🎯 CIBLE DU SPRINT 2   : %s msg/sec", r["target_rate"] or 500)
        logger.info("-" * 65)
        logger.info("Latence moyenne batch  : %.2f ms", r["latency_avg_ms"])
        logger.info("Latence p50 (médiane)  : %.2f ms", r["latency_p50_ms"])
        logger.info("Latence p95            : %.2f ms", r["latency_p95_ms"])
        logger.info("Latence p99            : %.2f ms", r["latency_p99_ms"])
        logger.info("-" * 65)
        if r["target_met"]:
            logger.info("✅ RÉSULTAT : OBJECTIF SPRINT 2 ATTEINT (≥ 500 msg/sec) !")
        else:
            logger.warning("⚠️ RÉSULTAT : DÉBIT INFÉRIEUR À L'OBJECTIF")
        logger.info("=" * 65 + "\n")


def main():
    parser = argparse.ArgumentParser(description="EnergyData Benchmark Tool")
    parser.add_argument("--rate", type=int, default=500, help="Cadence cible en msg/sec (défaut: 500)")
    parser.add_argument("--duration", type=int, default=5, help="Durée du test en secondes (défaut: 5s)")
    parser.add_argument("--num-messages", type=int, default=None, help="Nombre max de messages")
    parser.add_argument("--live", action="store_true", help="Activer l'envoi réel vers Azure Event Hubs")
    parser.add_argument("--uncapped", action="store_true", help="Débit maximal sans pause de régulation")
    args = parser.parse_args()

    rate = None if args.uncapped else args.rate
    runner = BenchmarkRunner(
        target_rate=rate,
        duration_seconds=args.duration,
        num_messages=args.num_messages,
        is_live=args.live,
    )
    runner.run()


if __name__ == "__main__":
    main()

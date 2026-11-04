"""
EnergyData Lakehouse — IoT Event Generator
===========================================
Sprint 1 | Membre A — Data Platform Engineer

Génère des événements télémétriques réalistes pour des compteurs
intelligents (Smart Meters) et sous-stations électriques.

Chaque événement est conforme au schéma JSON défini dans
IMPLEMENTATION_PLAN.md §3 (Contrat d'Interface 1).

Usage:
    python -m data_simulation.generator
    python -m data_simulation.generator --config config/simulation_config.yaml
    python -m data_simulation.generator --num-events 100 --output-file output/test.jsonl
"""

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import numpy as np
import yaml

from data_simulation.anomaly_injector import AnomalyInjector
from data_simulation.weather_enricher import WeatherEnricher

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DeviceSimulator — un capteur individuel
# ---------------------------------------------------------------------------
class DeviceSimulator:
    """
    Simule un seul compteur intelligent (Smart Meter).
    Maintient un état interne pour modéliser des tendances réalistes
    (charge de la journée, bruit corrélé dans le temps).
    """

    def __init__(
        self,
        device_id: str,
        substation_id: str,
        zone: str,
        latitude: float,
        longitude: float,
        physics_config: dict,
    ):
        self.device_id = device_id
        self.substation_id = substation_id
        self.zone = zone
        self.latitude = latitude
        self.longitude = longitude
        self.cfg = physics_config

        # État interne — bruit corrélé (random walk)
        self._voltage_offset: float = 0.0
        self._load_factor: float = random.uniform(0.3, 0.9)  # charge initiale aléatoire

    def _load_profile(self) -> float:
        """
        Simule un profil de charge journalier réaliste (courbe en cloche).
        Pic le matin (8h-9h) et le soir (18h-20h), creux la nuit.
        """
        hour = datetime.now(timezone.utc).hour
        # Double cloche : matin + soir
        morning_peak = np.exp(-0.5 * ((hour - 8.5) / 1.5) ** 2)
        evening_peak = np.exp(-0.5 * ((hour - 19.0) / 1.5) ** 2)
        base = 0.2
        return base + 0.5 * morning_peak + 0.4 * evening_peak

    def _update_state(self) -> None:
        """Met à jour l'état interne (random walk lent pour le voltage offset)."""
        self._voltage_offset += random.gauss(0, 0.3)
        self._voltage_offset = max(-10.0, min(10.0, self._voltage_offset))
        self._load_factor = self._load_profile() * random.uniform(0.9, 1.1)

    def generate_reading(self, ambient_temp: float) -> dict:
        """
        Génère une lecture télémérique pour ce capteur.

        Returns:
            dict: Payload JSON conforme au schéma IMPLEMENTATION_PLAN §3
        """
        self._update_state()
        cfg_v = self.cfg["voltage"]
        cfg_pf = self.cfg["power_factor"]
        cfg_f = self.cfg["frequency"]
        cfg_i = self.cfg["current"]

        # Tension : nominal + bruit gaussien + dérive lente
        voltage = (
            cfg_v["nominal_v"]
            + random.gauss(0, cfg_v["std_dev"])
            + self._voltage_offset
        )
        voltage = max(cfg_v["min_valid"], min(cfg_v["max_valid"], voltage))

        # Courant proportionnel au facteur de charge
        current = (
            cfg_i["min_a"]
            + (cfg_i["max_a"] - cfg_i["min_a"]) * self._load_factor
            + random.gauss(0, cfg_i["std_dev"])
        )
        current = max(0.0, current)

        # Facteur de puissance : nominal ± bruit gaussien
        power_factor = max(
            cfg_pf["min_valid"],
            min(cfg_pf["max_valid"], random.gauss(cfg_pf["nominal"], cfg_pf["std_dev"])),
        )

        # Puissance active : P = U × I × cos(φ) / 1000 (kW)
        active_power_kw = round((voltage * current * power_factor) / 1000.0, 3)

        # Puissance réactive : Q = P × tan(φ)
        import math
        phi = math.acos(power_factor) if power_factor < 1.0 else 0.0
        reactive_power_kvar = round(active_power_kw * math.tan(phi), 3)

        # Fréquence : nominal ± bruit gaussien très faible
        frequency = round(
            random.gauss(cfg_f["nominal_hz"], cfg_f["std_dev"]), 3
        )
        frequency = max(cfg_f["min_valid"], min(cfg_f["max_valid"], frequency))

        return {
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "substation_id": self.substation_id,
            "location": {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "zone": self.zone,
            },
            "metrics": {
                "voltage_v": round(voltage, 3),
                "current_a": round(current, 3),
                "active_power_kw": active_power_kw,
                "reactive_power_kvar": reactive_power_kvar,
                "power_factor": round(power_factor, 4),
                "frequency_hz": frequency,
                "ambient_temp_c": round(ambient_temp, 1),
            },
            "status_flag": "NORMAL",
        }


# ---------------------------------------------------------------------------
# IoTEventGenerator — Orchestrateur principal
# ---------------------------------------------------------------------------
class IoTEventGenerator:
    """
    Orchestre la simulation de N capteurs en parallèle.
    Charge la configuration, instancie les DeviceSimulators,
    injecte les anomalies et enrichit les données météo.
    """

    def __init__(self, config_path: str = "config/simulation_config.yaml"):
        self.config = self._load_config(config_path)
        self.devices = self._init_devices()
        self.anomaly_injector = AnomalyInjector(self.config["anomalies"])
        self.weather_enricher = WeatherEnricher(self.config.get("weather", {}))
        self._event_count = 0
        logger.info(
            "IoTEventGenerator initialized — %d devices, anomaly_rate=%.1f%%",
            len(self.devices),
            self.config["simulation"]["anomaly_rate"] * 100,
        )

    def _load_config(self, config_path: str) -> dict:
        """Charge et valide le fichier YAML de configuration."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.info("Configuration loaded from %s", config_path)
        return config

    def _init_devices(self) -> list[DeviceSimulator]:
        """Instancie tous les DeviceSimulators à partir de la configuration."""
        devices = []
        physics = self.config["physics"]
        prefix = self.config["devices"]["id_prefix"]
        idx = 1
        for zone_cfg in self.config["devices"]["zones"]:
            for _ in range(zone_cfg["num_devices"]):
                device_id = f"{prefix}_{zone_cfg['name'].upper()}_{idx:03d}"
                devices.append(
                    DeviceSimulator(
                        device_id=device_id,
                        substation_id=zone_cfg["substation_id"],
                        zone=zone_cfg["name"],
                        latitude=zone_cfg["latitude"],
                        longitude=zone_cfg["longitude"],
                        physics_config=physics,
                    )
                )
                idx += 1
        logger.info("Initialized %d device simulators across %d zones", len(devices), len(self.config["devices"]["zones"]))
        return devices

    def generate_batch(self) -> list[dict]:
        """
        Génère un batch d'événements pour tous les capteurs.
        Applique l'injection d'anomalies selon le taux configuré.

        Returns:
            list[dict]: Liste de payloads JSON prêts à l'envoi
        """
        anomaly_rate = self.config["simulation"]["anomaly_rate"]
        events = []

        for device in self.devices:
            # Récupération de la température (avec cache TTL)
            ambient_temp = self.weather_enricher.get_temperature(
                device.latitude, device.longitude
            )
            # Génération de la lecture normale
            event = device.generate_reading(ambient_temp)

            # Injection d'anomalie selon le taux configuré
            if random.random() < anomaly_rate:
                event = self.anomaly_injector.inject(event, self.config["physics"])

            events.append(event)
            self._event_count += 1

        return events

    def stream(
        self, num_batches: Optional[int] = None
    ) -> Generator[list[dict], None, None]:
        """
        Générateur infini (ou limité) de batches d'événements.

        Args:
            num_batches: Nombre de batches à générer. None = infini.

        Yields:
            list[dict]: Un batch d'événements
        """
        interval = self.config["simulation"]["emit_interval_seconds"]
        batch_num = 0
        while num_batches is None or batch_num < num_batches:
            start = time.time()
            batch = self.generate_batch()
            yield batch
            batch_num += 1
            # Respect de l'intervalle d'émission
            elapsed = time.time() - start
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def save_to_jsonl(self, events: list[dict], output_path: str) -> None:
        """Sauvegarde un batch d'événements dans un fichier JSONL local."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "a", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")

    @property
    def total_events_generated(self) -> int:
        return self._event_count


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="EnergyData IoT Event Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/simulation_config.yaml",
        help="Chemin vers le fichier de configuration YAML",
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=None,
        help="Nombre de batches à générer (défaut: infini)",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="Fichier JSONL de sortie locale (optionnel)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Génère 1 batch et affiche les événements sans les envoyer",
    )
    args = parser.parse_args()

    generator = IoTEventGenerator(config_path=args.config)

    if args.dry_run:
        logger.info("--- DRY RUN : génération d'un seul batch ---")
        batch = generator.generate_batch()
        for event in batch[:3]:
            print(json.dumps(event, indent=2, ensure_ascii=False))
        logger.info("Batch de %d événements générés. Exemple ci-dessus.", len(batch))
        return

    logger.info("Démarrage du streaming... (Ctrl+C pour arrêter)")
    try:
        for batch in generator.stream(num_batches=args.num_batches):
            if args.output_file:
                generator.save_to_jsonl(batch, args.output_file)

            # Log toutes les 10 batches
            normal = sum(1 for e in batch if e["status_flag"] == "NORMAL")
            anomalies = len(batch) - normal
            if generator.total_events_generated % (len(generator.devices) * 10) == 0:
                logger.info(
                    "Batch émis — total: %d | normal: %d | anomalies: %d",
                    generator.total_events_generated,
                    normal,
                    anomalies,
                )
    except KeyboardInterrupt:
        logger.info(
            "Simulation arrêtée. Total événements générés : %d",
            generator.total_events_generated,
        )


if __name__ == "__main__":
    main()

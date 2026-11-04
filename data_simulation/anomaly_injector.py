"""
EnergyData Lakehouse — Anomaly Injector
========================================
Sprint 1 | Membre A — Data Platform Engineer

Injecte des comportements anormaux contrôlés dans les payloads IoT.
Chaque type d'anomalie modifie les métriques physiques pour simuler
des pannes réalistes sur le réseau électrique.

Types d'anomalies supportés (définis dans simulation_config.yaml):
  - Overvoltage          : Tension supérieure à la normale (>250 V)
  - Undervoltage         : Tension inférieure à la normale (<200 V)
  - PowerFactorDrop      : Chute brutale du facteur de puissance (<0.8)
  - AbruptDisconnection  : Capteur muet pendant N cycles
  - FrequencyDeviation   : Fréquence hors plage nominale
"""

import copy
import logging
import random
from typing import Optional

logger = logging.getLogger(__name__)

# Compteur global des événements silencieux par device (pour AbruptDisconnection)
_silent_counters: dict[str, int] = {}


class AnomalyInjector:
    """
    Injecte des anomalies dans les événements IoT selon la configuration.
    
    La sélection de l'anomalie est pondérée par les probabilités relatives
    définies dans simulation_config.yaml → anomalies.types.
    """

    def __init__(self, anomaly_config: dict):
        """
        Args:
            anomaly_config: Section 'anomalies' du fichier de configuration YAML.
        """
        self.types = anomaly_config.get("types", [])
        # Normalisation des poids pour la sélection aléatoire pondérée
        total_prob = sum(t["probability"] for t in self.types)
        self._weights = [t["probability"] / total_prob for t in self.types]
        logger.debug(
            "AnomalyInjector initialized with %d anomaly types", len(self.types)
        )

    def _select_anomaly_type(self) -> Optional[dict]:
        """Sélectionne un type d'anomalie selon les poids configurés."""
        if not self.types:
            return None
        return random.choices(self.types, weights=self._weights, k=1)[0]

    def inject(self, event: dict, physics_config: dict) -> dict:
        """
        Injecte une anomalie dans un événement IoT.

        Args:
            event: Payload JSON d'origine (sera copié, non modifié en place).
            physics_config: Paramètres physiques pour validation des plages.

        Returns:
            dict: Payload modifié avec l'anomalie injectée et status_flag mis à jour.
        """
        anomaly_type = self._select_anomaly_type()
        if anomaly_type is None:
            return event

        # Copie profonde pour ne pas modifier l'original
        anomalous_event = copy.deepcopy(event)
        anomaly_name = anomaly_type["name"]
        device_id = event["device_id"]

        if anomaly_name == "Overvoltage":
            anomalous_event = self._inject_overvoltage(anomalous_event, anomaly_type)

        elif anomaly_name == "Undervoltage":
            anomalous_event = self._inject_undervoltage(anomalous_event, anomaly_type)

        elif anomaly_name == "PowerFactorDrop":
            anomalous_event = self._inject_power_factor_drop(anomalous_event, anomaly_type)

        elif anomaly_name == "AbruptDisconnection":
            anomalous_event = self._inject_disconnection(
                anomalous_event, anomaly_type, device_id
            )

        elif anomaly_name == "FrequencyDeviation":
            anomalous_event = self._inject_frequency_deviation(anomalous_event, anomaly_type)

        else:
            logger.warning("Unknown anomaly type: %s — skipping injection", anomaly_name)
            return event

        # Mise à jour du flag de statut
        if anomalous_event.get("status_flag") != "DISCONNECTED":
            anomalous_event["status_flag"] = "ANOMALY"
        anomalous_event["anomaly_type"] = anomaly_name

        logger.debug(
            "Anomaly injected: device=%s type=%s", device_id, anomaly_name
        )
        return anomalous_event

    # ------------------------------------------------------------------
    # Méthodes privées d'injection par type
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_overvoltage(event: dict, cfg: dict) -> dict:
        """Multiplie la tension par un facteur > 1 (ex: 1.15 → 264.5V pour 230V nominal)."""
        multiplier = cfg.get("voltage_multiplier", 1.15)
        original_v = event["metrics"]["voltage_v"]
        new_v = round(original_v * multiplier + random.uniform(0, 5), 3)
        event["metrics"]["voltage_v"] = new_v
        # La puissance active augmente proportionnellement
        event["metrics"]["active_power_kw"] = round(
            event["metrics"]["active_power_kw"] * multiplier, 3
        )
        return event

    @staticmethod
    def _inject_undervoltage(event: dict, cfg: dict) -> dict:
        """Multiplie la tension par un facteur < 1 (ex: 0.80 → 184V pour 230V nominal)."""
        multiplier = cfg.get("voltage_multiplier", 0.80)
        original_v = event["metrics"]["voltage_v"]
        new_v = round(original_v * multiplier - random.uniform(0, 5), 3)
        new_v = max(50.0, new_v)  # plancher physique
        event["metrics"]["voltage_v"] = new_v
        event["metrics"]["active_power_kw"] = round(
            event["metrics"]["active_power_kw"] * multiplier, 3
        )
        return event

    @staticmethod
    def _inject_power_factor_drop(event: dict, cfg: dict) -> dict:
        """Remplace le power factor par une valeur anormalement basse."""
        import math
        pf_value = cfg.get("power_factor_value", 0.65)
        # Ajouter un peu de bruit autour de la valeur cible
        pf = max(0.1, min(0.79, pf_value + random.gauss(0, 0.03)))
        event["metrics"]["power_factor"] = round(pf, 4)
        # Recalculer la puissance réactive
        try:
            phi = math.acos(pf)
            event["metrics"]["reactive_power_kvar"] = round(
                event["metrics"]["active_power_kw"] * math.tan(phi), 3
            )
        except ValueError:
            pass  # pf hors domaine — ne pas recalculer
        return event

    @staticmethod
    def _inject_disconnection(event: dict, cfg: dict, device_id: str) -> dict:
        """
        Simule une déconnexion brusque : toutes les métriques tombent à 0.
        Le capteur reste 'silencieux' pendant N cycles.
        """
        global _silent_counters
        silent_cycles = cfg.get("silent_cycles", 3)

        # Initialiser ou décrémenter le compteur de silence
        if device_id not in _silent_counters:
            _silent_counters[device_id] = silent_cycles

        if _silent_counters[device_id] > 0:
            _silent_counters[device_id] -= 1
            # Mise à zéro de toutes les métriques
            for key in event["metrics"]:
                if key != "ambient_temp_c":
                    event["metrics"][key] = 0.0
            event["status_flag"] = "DISCONNECTED"
        else:
            # Réinitialiser après les cycles silencieux
            del _silent_counters[device_id]

        return event

    @staticmethod
    def _inject_frequency_deviation(event: dict, cfg: dict) -> dict:
        """Décale la fréquence hors de la plage nominale (48-52 Hz)."""
        offset = cfg.get("frequency_offset", 2.5)
        # Direction aléatoire (+/-)
        direction = random.choice([-1, 1])
        new_freq = round(
            event["metrics"]["frequency_hz"] + direction * offset + random.uniform(0, 0.5),
            3,
        )
        # Clamp dans une plage extrême réaliste
        new_freq = max(45.0, min(55.0, new_freq))
        event["metrics"]["frequency_hz"] = new_freq
        return event

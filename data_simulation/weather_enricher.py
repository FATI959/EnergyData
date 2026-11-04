"""
EnergyData Lakehouse — Weather Enricher
========================================
Sprint 1 | Membre A — Data Platform Engineer

Connecteur vers l'API Open-Meteo (gratuite, sans clé API).
Enrichit les événements IoT avec la température ambiante réelle
selon la position géographique (latitude/longitude) du capteur.

Fonctionnalités:
  - Requête HTTP vers l'API Open-Meteo
  - Cache TTL configurable (défaut: 300 secondes = 5 minutes)
  - Fallback gracieux si l'API est indisponible
  - Clé de cache par position géographique (arrondie à 2 décimales)
"""

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Résolution du cache géographique (degrés)
GEO_PRECISION = 2  # Arrondi à 0.01° ≈ ~1 km


class WeatherEnricher:
    """
    Enrichit les événements IoT avec la température ambiante via Open-Meteo.
    
    Utilise un cache en mémoire (dict) pour éviter de saturer l'API.
    La clé de cache est (lat_rounded, lon_rounded) pour regrouper
    les capteurs géographiquement proches.
    """

    def __init__(self, weather_config: dict):
        """
        Args:
            weather_config: Section 'weather' du fichier de configuration YAML.
        """
        self.api_url = weather_config.get(
            "api_url", "https://api.open-meteo.com/v1/forecast"
        )
        self.fallback_temp = weather_config.get("fallback_temp_c", 18.0)
        self.cache_ttl = weather_config.get("cache_ttl_seconds", 300)

        # Cache : {(lat, lon) -> (temperature, timestamp)}
        self._cache: dict[tuple, tuple[float, float]] = {}

        logger.info(
            "WeatherEnricher initialized — API: %s | TTL: %ds | fallback: %.1f°C",
            self.api_url,
            self.cache_ttl,
            self.fallback_temp,
        )

    def _cache_key(self, latitude: float, longitude: float) -> tuple:
        """Génère une clé de cache normalisée par position géographique."""
        return (round(latitude, GEO_PRECISION), round(longitude, GEO_PRECISION))

    def _is_cache_valid(self, key: tuple) -> bool:
        """Vérifie si la valeur en cache est encore valide (non expirée)."""
        if key not in self._cache:
            return False
        _, cached_at = self._cache[key]
        return (time.time() - cached_at) < self.cache_ttl

    def _fetch_temperature(self, latitude: float, longitude: float) -> Optional[float]:
        """
        Interroge l'API Open-Meteo pour la température actuelle.

        Args:
            latitude: Latitude du capteur.
            longitude: Longitude du capteur.

        Returns:
            Température en °C, ou None si l'API est indisponible.
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current_weather": "true",
            "hourly": "temperature_2m",
            "forecast_days": 1,
        }
        try:
            response = requests.get(self.api_url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()

            # Priorité : current_weather, sinon premier point horaire
            if "current_weather" in data:
                temp = data["current_weather"].get("temperature")
                if temp is not None:
                    return float(temp)

            if "hourly" in data and "temperature_2m" in data["hourly"]:
                temps = data["hourly"]["temperature_2m"]
                if temps:
                    return float(temps[0])

        except requests.exceptions.ConnectionError:
            logger.warning("WeatherEnricher — No network connection. Using fallback.")
        except requests.exceptions.Timeout:
            logger.warning("WeatherEnricher — API timeout (%.0fs). Using fallback.", 5)
        except requests.exceptions.RequestException as e:
            logger.warning("WeatherEnricher — API error: %s. Using fallback.", e)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("WeatherEnricher — Parse error: %s. Using fallback.", e)
        except Exception as e:
            logger.warning("WeatherEnricher — Unexpected error: %s. Using fallback.", e)

        return None

    def get_temperature(self, latitude: float, longitude: float) -> float:
        """
        Retourne la température ambiante pour une position géographique.

        Utilise le cache si disponible, sinon interroge l'API.
        En cas d'échec de l'API, retourne la valeur de fallback.

        Args:
            latitude: Latitude du capteur.
            longitude: Longitude du capteur.

        Returns:
            Température en °C (float).
        """
        key = self._cache_key(latitude, longitude)

        # Vérification du cache
        if self._is_cache_valid(key):
            temp, _ = self._cache[key]
            return temp

        # Appel API
        temp = self._fetch_temperature(latitude, longitude)

        if temp is not None:
            self._cache[key] = (temp, time.time())
            logger.debug(
                "WeatherEnricher — Fetched %.1f°C for (%.2f, %.2f)",
                temp, latitude, longitude,
            )
            return temp

        # Fallback mis en cache pour éviter la surcharge réseau en cas de coupure
        self._cache[key] = (self.fallback_temp, time.time())
        logger.debug(
            "WeatherEnricher — Using fallback %.1f°C for (%.2f, %.2f)",
            self.fallback_temp, latitude, longitude,
        )
        return self.fallback_temp

    def clear_cache(self) -> None:
        """Vide le cache (utile pour les tests)."""
        self._cache.clear()
        logger.debug("WeatherEnricher cache cleared.")

    @property
    def cache_size(self) -> int:
        """Nombre d'entrées actuelles dans le cache."""
        return len(self._cache)

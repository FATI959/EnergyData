# Databricks notebook source
# =============================================================================
# Sprint 5 — Feature Store ML : Extraction & Préparation des Features
# Auteur  : Membre A — Data Platform Engineer
# Branch  : feature/memberA-gold
# =============================================================================
#
# Pipeline :
#   Gold (Delta, /mnt/lakehouse/gold/energy_5min_aggregates)
#       └─► Calcul des features statistiques (rolling, lag, z-score)
#               └─► Feature Store : /mnt/lakehouse/silver/ml_features
#
# Features produites (Contrat d'Interface ML — Sprint 6) :
#   • avg_active_power_kw      — puissance moyenne fenêtre 5-min
#   • peak_active_power_kw     — puissance de pointe
#   • total_energy_kwh         — énergie consommée
#   • avg_power_factor         — facteur de puissance moyen
#   • voltage_min / voltage_max — plage de tension
#   • voltage_range            — amplitude tension (max - min)
#   • power_factor_deviation   — écart au facteur de puissance idéal (1.0)
#   • rolling_avg_power_3w     — moyenne glissante 3 fenêtres (15 min)
#   • rolling_std_power_3w     — écart-type glissant 3 fenêtres
#   • lag_1_power              — puissance à t-1 (5 min avant)
#   • lag_3_power              — puissance à t-3 (15 min avant)
#   • power_delta              — variation de puissance vs fenêtre précédente
#   • z_score_power            — score Z de la puissance (détection anomalies)
#   • hour_of_day              — heure du jour (0-23) pour saisonnalité
#   • day_of_week              — jour de la semaine (0=lundi, 6=dimanche)
#   • is_peak_hour             — indicateur heure de pointe (8h-20h)
# =============================================================================

# COMMAND ----------
# Cellule 1 — Imports

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
)

# COMMAND ----------
# Cellule 2 — Configuration du logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("05_ml_feature_store")

# COMMAND ----------
# Cellule 3 — Session Spark


def get_spark_session() -> SparkSession:
    """Récupère la session Spark active ou en crée une (mode local pour tests)."""
    return (
        SparkSession.builder.appName("EnergyData_ML_Feature_Store")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


spark = get_spark_session()
logger.info("Session Spark initialisée : %s", spark.version)

# COMMAND ----------
# Cellule 4 — Paramètres des chemins

GOLD_AGGREGATES_PATH = "/mnt/lakehouse/gold/energy_5min_aggregates"
ML_FEATURES_PATH = "/mnt/lakehouse/silver/ml_features"
CHECKPOINT_ML_FEATURES = "/mnt/lakehouse/checkpoints/ml_features"

# Nombre de fenêtres pour le rolling (3 × 5 min = 15 min)
ROLLING_WINDOW_SIZE = 3
# Heure de début / fin des heures de pointe
PEAK_HOUR_START = 8
PEAK_HOUR_END = 20

logger.info("Feature Store configuré → %s", ML_FEATURES_PATH)

# COMMAND ----------
# Cellule 5 — Chargement de la configuration


def load_config(config_path: str = "config/databricks_config.json") -> dict[str, Any]:
    """Charge la configuration du projet (chemins, paramètres)."""
    import json

    try:
        with open(config_path, encoding="utf-8") as fh:
            cfg = json.load(fh)
        logger.info("Configuration chargée depuis : %s", config_path)
        return cfg
    except FileNotFoundError:
        logger.warning("Config introuvable (%s) — utilisation des valeurs par défaut.", config_path)
        return {}


# COMMAND ----------
# Cellule 6 — Lecture de la couche Gold (batch)


def read_gold_aggregates(spark_session: SparkSession, path: str) -> DataFrame:
    """
    Lit la table Gold des agrégats 5-min en mode batch depuis Delta Lake.

    Colonnes attendues (Contrat d'Interface 3) :
        substation_id, zone, window_start, window_end,
        avg_active_power_kw, peak_active_power_kw, total_energy_kwh,
        avg_power_factor, voltage_min, voltage_max
    """
    logger.info("Lecture Gold agrégats depuis : %s", path)
    df = spark_session.read.format("delta").load(path)
    logger.info("Gold chargé — %d lignes, %d colonnes", df.count(), len(df.columns))
    return df


# COMMAND ----------
# Cellule 7 — Ingénierie des features de base


def build_base_features(gold_df: DataFrame) -> DataFrame:
    """
    Calcule les features dérivées directement depuis les agrégats Gold.

    Features ajoutées :
        • voltage_range          = voltage_max - voltage_min
        • power_factor_deviation = abs(1.0 - avg_power_factor)
        • hour_of_day            = heure extraite de window_start
        • day_of_week            = jour de la semaine (0=lundi)
        • is_peak_hour           = True si 8h ≤ heure < 20h
    """
    logger.info("Construction des features de base...")
    return gold_df.withColumn(
        "voltage_range",
        F.round(F.col("voltage_max") - F.col("voltage_min"), 3),
    ).withColumn(
        "power_factor_deviation",
        F.round(F.abs(F.lit(1.0) - F.col("avg_power_factor")), 4),
    ).withColumn(
        "hour_of_day",
        F.hour(F.col("window_start")).cast(IntegerType()),
    ).withColumn(
        "day_of_week",
        F.dayofweek(F.col("window_start")).cast(IntegerType()),
    ).withColumn(
        "is_peak_hour",
        (
            (F.col("hour_of_day") >= PEAK_HOUR_START)
            & (F.col("hour_of_day") < PEAK_HOUR_END)
        ).cast(BooleanType()),
    )


# COMMAND ----------
# Cellule 8 — Features temporelles : rolling & lag


def build_temporal_features(base_df: DataFrame) -> DataFrame:
    """
    Calcule les features de série temporelle par substation.

    Partitionnement : substation_id  (chaque sous-station a son propre historique)
    Tri            : window_start ASC

    Features ajoutées :
        • rolling_avg_power_3w  — moyenne glissante 3 fenêtres de avg_active_power_kw
        • rolling_std_power_3w  — écart-type glissant 3 fenêtres
        • lag_1_power           — avg_active_power_kw à t-1 (fenêtre précédente)
        • lag_3_power           — avg_active_power_kw à t-3
        • power_delta           — différence vs fenêtre précédente (trend)
    """
    logger.info("Construction des features temporelles (rolling & lag)...")

    # Fenêtre glissante : 3 lignes précédentes incluse la courante
    w_rolling = (
        Window.partitionBy("substation_id")
        .orderBy("window_start")
        .rowsBetween(-(ROLLING_WINDOW_SIZE - 1), 0)
    )

    # Fenêtre de lag (accès aux lignes précédentes)
    w_lag = Window.partitionBy("substation_id").orderBy("window_start")

    return (
        base_df
        # Rolling mean & std sur 3 fenêtres (15 min)
        .withColumn(
            "rolling_avg_power_3w",
            F.round(F.avg("avg_active_power_kw").over(w_rolling), 4),
        )
        .withColumn(
            "rolling_std_power_3w",
            F.round(F.stddev("avg_active_power_kw").over(w_rolling), 4),
        )
        # Lag features
        .withColumn(
            "lag_1_power",
            F.lag("avg_active_power_kw", 1).over(w_lag),
        )
        .withColumn(
            "lag_3_power",
            F.lag("avg_active_power_kw", 3).over(w_lag),
        )
        # Delta de puissance (variation vs t-1)
        .withColumn(
            "power_delta",
            F.round(F.col("avg_active_power_kw") - F.col("lag_1_power"), 4),
        )
    )


# COMMAND ----------
# Cellule 9 — Feature Z-score (détection d'anomalies)


def build_zscore_feature(temporal_df: DataFrame) -> DataFrame:
    """
    Calcule le Z-score de la puissance active par sous-station.

    Formule :
        z_score = (avg_active_power_kw - rolling_avg) / rolling_std

    Un |z_score| > 3  →  anomalie potentielle (seuil standard 3-sigma).
    Cette feature sera la cible principale de l'Isolation Forest (Sprint 6).
    """
    logger.info("Calcul du Z-score de puissance...")
    return temporal_df.withColumn(
        "z_score_power",
        F.round(
            F.when(
                # Évite la division par zéro quand std = 0 ou NULL
                (F.col("rolling_std_power_3w").isNull())
                | (F.col("rolling_std_power_3w") == 0),
                F.lit(0.0),
            ).otherwise(
                (F.col("avg_active_power_kw") - F.col("rolling_avg_power_3w"))
                / F.col("rolling_std_power_3w")
            ),
            4,
        ),
    )


# COMMAND ----------
# Cellule 10 — Sélection finale & schéma du Feature Store


def select_feature_store_columns(zscore_df: DataFrame) -> DataFrame:
    """
    Sélectionne et ordonne les colonnes du Feature Store ML.

    Schéma final (Contrat d'Interface ML pour Sprint 6) :
        substation_id, zone, window_start, window_end,
        avg_active_power_kw, peak_active_power_kw, total_energy_kwh,
        avg_power_factor, voltage_min, voltage_max,
        voltage_range, power_factor_deviation,
        rolling_avg_power_3w, rolling_std_power_3w,
        lag_1_power, lag_3_power, power_delta, z_score_power,
        hour_of_day, day_of_week, is_peak_hour
    """
    logger.info("Sélection des colonnes finales du Feature Store...")
    return zscore_df.select(
        # ── Clés de partitionnement ─────────────────────────────────────────
        "substation_id",
        "zone",
        "window_start",
        "window_end",
        # ── Features Gold (brutes) ──────────────────────────────────────────
        "avg_active_power_kw",
        "peak_active_power_kw",
        "total_energy_kwh",
        "avg_power_factor",
        "voltage_min",
        "voltage_max",
        # ── Features dérivées de base ───────────────────────────────────────
        "voltage_range",
        "power_factor_deviation",
        # ── Features temporelles ────────────────────────────────────────────
        "rolling_avg_power_3w",
        "rolling_std_power_3w",
        "lag_1_power",
        "lag_3_power",
        "power_delta",
        "z_score_power",
        # ── Features calendaires ────────────────────────────────────────────
        "hour_of_day",
        "day_of_week",
        "is_peak_hour",
    )


# COMMAND ----------
# Cellule 11 — Écriture dans le Feature Store (Delta, mode overwrite)


def write_feature_store(features_df: DataFrame, output_path: str) -> None:
    """
    Persiste le Feature Store ML en Delta Lake (mode overwrite).

    Partitionnement : substation_id  (aligné avec les tables Gold)
    Mode           : overwrite (re-calcul complet à chaque run)
    """
    logger.info("Écriture du Feature Store → %s", output_path)
    (
        features_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("substation_id")
        .save(output_path)
    )
    logger.info("Feature Store écrit avec succès.")


# COMMAND ----------
# Cellule 12 — Affichage des statistiques descriptives


def display_feature_stats(features_df: DataFrame) -> None:
    """
    Affiche un résumé statistique des features pour validation.
    Appelé après l'écriture pour vérification rapide en Databricks.
    """
    logger.info("=== Statistiques descriptives du Feature Store ===")

    numeric_cols = [
        "avg_active_power_kw",
        "voltage_range",
        "power_factor_deviation",
        "rolling_avg_power_3w",
        "rolling_std_power_3w",
        "z_score_power",
        "power_delta",
    ]

    stats = features_df.select(numeric_cols).describe()
    stats.show(truncate=False)

    # Nombre de lignes avec z_score absolu > 3 (anomalies potentielles)
    anomaly_count = features_df.filter(
        F.abs(F.col("z_score_power")) > 3
    ).count()

    total_count = features_df.count()
    anomaly_pct = (anomaly_count / total_count * 100) if total_count > 0 else 0.0

    logger.info(
        "Anomalies potentielles (|z_score| > 3) : %d / %d (%.2f%%)",
        anomaly_count,
        total_count,
        anomaly_pct,
    )


# COMMAND ----------
# Cellule 13 — Pipeline principal Feature Store


def run_feature_store_pipeline(spark_session: SparkSession) -> DataFrame:
    """
    Point d'entrée complet du pipeline Feature Store ML.

    Étapes :
        1. Lecture Gold (batch)
        2. Features de base (voltage_range, power_factor_deviation, calendaire)
        3. Features temporelles (rolling, lag, delta)
        4. Z-score de puissance
        5. Sélection & typage des colonnes finales
        6. Écriture Delta Lake
        7. Statistiques de validation
    """
    logger.info("=== Démarrage Pipeline Feature Store ML ===")

    # 1. Lecture
    gold_df = read_gold_aggregates(spark_session, GOLD_AGGREGATES_PATH)

    # 2-4. Ingénierie des features (pipeline en cascade)
    features_df = (
        gold_df
        .transform(build_base_features)
        .transform(build_temporal_features)
        .transform(build_zscore_feature)
        .transform(select_feature_store_columns)
    )

    # 5. Écriture
    write_feature_store(features_df, ML_FEATURES_PATH)

    # 6. Validation
    display_feature_stats(features_df)

    logger.info("=== Pipeline Feature Store ML terminé ===")
    return features_df


# COMMAND ----------
# Cellule 14 — Exécution

features = run_feature_store_pipeline(spark)

# COMMAND ----------
# Cellule 15 — Conseil d'optimisation
#
# Z-ORDER pour accélérer les lectures ML (Isolation Forest, Prophet) :
#
#   OPTIMIZE delta.`/mnt/lakehouse/silver/ml_features`
#   ZORDER BY (substation_id, window_start);
#
# À exécuter après le premier chargement complet (run quotidien recommandé).

logger.info(
    "Conseil : exécutez OPTIMIZE ... ZORDER BY (substation_id, window_start) "
    "pour accélérer l'entraînement ML sur le Feature Store."
)

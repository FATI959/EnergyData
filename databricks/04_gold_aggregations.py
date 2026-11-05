# Databricks notebook source
# =============================================================================
# Sprint 5 — Couche Gold : Agrégations 5-min & KPIs Métier
# Auteur  : Membre A — Data Platform Engineer
# Branch  : feature/memberA-gold
# =============================================================================
#
# Pipeline :
#   Silver (Delta, /mnt/lakehouse/silver/energy_telemetry)
#       └─► Fenêtres glissantes 5 min (Structured Streaming)
#               └─► Gold : gold_energy_5min_aggregates
#
# Tables Gold vides initialisées pour le ML (Sprint 6) :
#       • gold_detected_anomalies
#       • gold_load_forecasts
#
# Schémas conformes au Contrat d'Interface 3 (IMPLEMENTATION_PLAN.md)
# =============================================================================

# COMMAND ----------
# Cellule 1 — Imports

import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------
# Cellule 2 — Configuration du logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("04_gold_aggregations")

# COMMAND ----------
# Cellule 3 — Récupération sécurisée de dbutils (évite les erreurs Pylance)

dbutils = globals().get("dbutils", None)  # type: ignore[assignment]


def get_secret(scope: str, key: str, fallback_env_var: str = "") -> str:
    """Récupère un secret depuis Databricks Secrets ou les variables d'environnement."""
    try:
        if dbutils is not None:
            return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass
    return os.getenv(fallback_env_var, "")


# COMMAND ----------
# Cellule 4 — Session Spark


def get_spark_session() -> SparkSession:
    """Récupère la session Spark active ou en crée une (mode local pour tests)."""
    return (
        SparkSession.builder.appName("EnergyData_Gold_Aggregations")
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
# Cellule 5 — Paramètres des chemins (issus de databricks_config.json)

SILVER_PATH = "/mnt/lakehouse/silver/energy_telemetry"
GOLD_AGGREGATES_PATH = "/mnt/lakehouse/gold/energy_5min_aggregates"
GOLD_ANOMALIES_PATH = "/mnt/lakehouse/gold/detected_anomalies"
GOLD_FORECASTS_PATH = "/mnt/lakehouse/gold/load_forecasts"
CHECKPOINT_GOLD = "/mnt/lakehouse/checkpoints/gold_aggregations"

# Fenêtre de watermark pour absorber les événements tardifs (15 min)
WATERMARK_DELAY = "15 minutes"
# Durée de la fenêtre glissante
WINDOW_DURATION = "5 minutes"
# Intervalle de slide (ici tumbling = slide == window)
SLIDE_DURATION = "5 minutes"

logger.info("Chemins Gold configurés — agrégats : %s", GOLD_AGGREGATES_PATH)

# COMMAND ----------
# Cellule 6 — Lecture du flux Silver (Structured Streaming, format Delta)


def read_silver_stream(spark_session: SparkSession, path: str) -> DataFrame:
    """
    Lit la table Silver en mode streaming depuis Delta Lake.

    Colonnes attendues (Contrat d'Interface 2) :
        event_time (timestamp), device_id, substation_id, zone,
        voltage_v, current_a, active_power_kw, reactive_power_kvar,
        power_factor, frequency_hz, ambient_temp_c, is_valid_reading
    """
    logger.info("Lecture du flux Silver depuis : %s", path)
    return (
        spark_session.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(path)
        .filter(F.col("is_valid_reading") == True)  # noqa: E712
    )


# COMMAND ----------
# Cellule 7 — Transformation : agrégations sur fenêtres 5-min


def build_gold_aggregations(silver_df: DataFrame) -> DataFrame:
    """
    Calcule les KPIs métier sur des fenêtres glissantes de 5 minutes.

    Formule énergie :
        total_energy_kwh = avg_active_power_kw × (window_minutes / 60)
        Pour une fenêtre de 5 min  →  avg_kw × (5/60)

    Colonnes produites (Contrat d'Interface 3 — gold_energy_5min_aggregates) :
        substation_id, zone, window_start, window_end,
        avg_active_power_kw, peak_active_power_kw, total_energy_kwh,
        avg_power_factor, voltage_min, voltage_max
    """
    window_minutes: float = 5.0

    with_watermark = silver_df.withWatermark("event_time", WATERMARK_DELAY)

    aggregated = with_watermark.groupBy(
        F.col("substation_id"),
        F.col("zone"),
        F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
    ).agg(
        F.avg("active_power_kw").alias("avg_active_power_kw"),
        F.max("active_power_kw").alias("peak_active_power_kw"),
        (F.avg("active_power_kw") * (window_minutes / 60.0)).alias("total_energy_kwh"),
        F.avg("power_factor").alias("avg_power_factor"),
        F.min("voltage_v").alias("voltage_min"),
        F.max("voltage_v").alias("voltage_max"),
    )

    # Aplatir la colonne struct window → window_start / window_end
    return aggregated.select(
        F.col("substation_id"),
        F.col("zone"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.round(F.col("avg_active_power_kw"), 4).alias("avg_active_power_kw"),
        F.round(F.col("peak_active_power_kw"), 4).alias("peak_active_power_kw"),
        F.round(F.col("total_energy_kwh"), 6).alias("total_energy_kwh"),
        F.round(F.col("avg_power_factor"), 4).alias("avg_power_factor"),
        F.round(F.col("voltage_min"), 2).alias("voltage_min"),
        F.round(F.col("voltage_max"), 2).alias("voltage_max"),
    )


# COMMAND ----------
# Cellule 8 — Écriture en streaming vers la table Gold Delta


def write_gold_stream(gold_df: DataFrame, output_path: str, checkpoint_path: str):
    """
    Écrit le flux agrégé vers Delta Lake en mode 'append'.

    Partitionnement : substation_id  (facilite les lectures Power BI DirectQuery)
    Trigger       : processingTime 1 minute (compromis latence / coût cluster)
    """
    logger.info("Démarrage de l'écriture Gold vers : %s", output_path)
    query = (
        gold_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("substation_id")
        .trigger(processingTime="1 minute")
        .start(output_path)
    )
    logger.info(
        "Streaming query démarrée — ID : %s | État : %s",
        query.id,
        query.status,
    )
    return query


# COMMAND ----------
# Cellule 9 — Pipeline principal Gold Agrégations


def run_gold_aggregations_pipeline(spark_session: SparkSession) -> None:
    """Point d'entrée du pipeline Gold — agrégations 5-min."""
    logger.info("=== Démarrage Pipeline Gold — Agrégations 5-min ===")

    # 1. Lecture Silver
    silver_stream = read_silver_stream(spark_session, SILVER_PATH)

    # 2. Agrégations fenêtrées
    gold_df = build_gold_aggregations(silver_stream)

    # 3. Écriture Gold
    query = write_gold_stream(gold_df, GOLD_AGGREGATES_PATH, CHECKPOINT_GOLD)

    logger.info("Pipeline Gold actif — query id : %s", query.id)


# COMMAND ----------
# Cellule 10 — Initialisation des tables Gold vides pour le ML (Sprint 6)
#
# Ces tables sont créées une seule fois avec le schéma cible.
# Le Sprint 6 (ML Engine) les alimentera via UDF Spark / batch scoring.


def init_ml_tables(spark_session: SparkSession) -> None:
    """
    Crée les tables Gold vides pour les résultats ML.

    Tables créées :
        • gold_detected_anomalies  — scoring Isolation Forest (stream scoring)
        • gold_load_forecasts      — prévisions H+24 / J+7 (Prophet / XGBoost)

    Si les tables existent déjà (runs suivants), la fonction est no-op.
    """
    # ── Schéma : gold_detected_anomalies ─────────────────────────────────────
    anomalies_schema = StructType(
        [
            StructField("anomaly_id", StringType(), False),   # UUID v4
            StructField("device_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("anomaly_type", StringType(), True),  # voltage_drop, spike…
            StructField("severity", StringType(), True),      # Low / Medium / Critical
            StructField("z_score", DoubleType(), True),
        ]
    )

    # ── Schéma : gold_load_forecasts ─────────────────────────────────────────
    forecasts_schema = StructType(
        [
            StructField("forecast_id", StringType(), False),  # UUID v4
            StructField("substation_id", StringType(), False),
            StructField("target_timestamp", TimestampType(), False),
            StructField("predicted_active_power_kw", DoubleType(), True),
            StructField("lower_bound_kw", DoubleType(), True),
            StructField("upper_bound_kw", DoubleType(), True),
            StructField("model_version", StringType(), True),
        ]
    )

    for schema, path, table_name in [
        (anomalies_schema, GOLD_ANOMALIES_PATH, "gold_detected_anomalies"),
        (forecasts_schema, GOLD_FORECASTS_PATH, "gold_load_forecasts"),
    ]:
        try:
            if DeltaTable.isDeltaTable(spark_session, path):
                logger.info("Table '%s' existe déjà — skip.", table_name)
                continue
        except Exception:
            pass  # DeltaTable.isDeltaTable peut lever en mode local

        empty_df = spark_session.createDataFrame([], schema)
        (
            empty_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        logger.info("Table Gold vide créée : %s → %s", table_name, path)


# COMMAND ----------
# Cellule 11 — Exécution

run_gold_aggregations_pipeline(spark)
init_ml_tables(spark)

# COMMAND ----------
# Cellule 12 — Optimisation batch (à exécuter manuellement après chargement initial)
#
# Z-ORDER améliore les performances des requêtes Power BI DirectQuery
# en co-localisant les données filtrées par substation_id et window_start.
#
# SQL à exécuter dans un notebook séparé ou via Databricks SQL Warehouse :
#
#   OPTIMIZE delta.`/mnt/lakehouse/gold/energy_5min_aggregates`
#   ZORDER BY (substation_id, window_start);
#
# Fréquence recommandée : 1 fois par jour (hors heures de pointe)

logger.info(
    "Conseil : exécutez OPTIMIZE ... ZORDER BY (substation_id, window_start) "
    "quotidiennement pour optimiser les performances DirectQuery Power BI."
)

# Databricks notebook source
"""
EnergyData Lakehouse Platform — Nettoyage, Déduplication & Couche Silver
========================================================================
Sprint 3 | Membre A — Data Platform Engineer

Pipeline Spark Structured Streaming consommant la table Delta Bronze,
réalisant le parsing strict du JSON, l'application du Watermarking (15 min),
la déduplication des messages, la validation de plausibilité physique
et le calcul de la latence d'ingestion.

Schéma Silver généré (Contrat d'Interface 2):
  - device_id              : STRING
  - substation_id          : STRING
  - zone                   : STRING
  - event_time             : TIMESTAMP
  - voltage_v              : DOUBLE
  - current_a              : DOUBLE
  - active_power_kw        : DOUBLE
  - reactive_power_kvar    : DOUBLE
  - power_factor           : DOUBLE
  - frequency_hz           : DOUBLE
  - ambient_temp_c         : DOUBLE
  - status_flag            : STRING
  - is_valid_reading       : BOOLEAN
  - ingestion_latency_sec  : DOUBLE
  - ingestion_timestamp    : TIMESTAMP
"""

import json
import logging
import os
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
    unix_timestamp,
    when,
    lit,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("02_silver_cleaning")


# ---------------------------------------------------------------------------
# Schéma JSON du Payload IoT (Contrat d'Interface 1)
# ---------------------------------------------------------------------------
IOT_PAYLOAD_SCHEMA = StructType([
    StructField("device_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("substation_id", StringType(), False),
    StructField(
        "location",
        StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("zone", StringType(), True),
        ]),
        True,
    ),
    StructField(
        "metrics",
        StructType([
            StructField("voltage_v", DoubleType(), True),
            StructField("current_a", DoubleType(), True),
            StructField("active_power_kw", DoubleType(), True),
            StructField("reactive_power_kvar", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("frequency_hz", DoubleType(), True),
            StructField("ambient_temp_c", DoubleType(), True),
        ]),
        True,
    ),
    StructField("status_flag", StringType(), True),
])


# ---------------------------------------------------------------------------
# Initialisation session Spark
# ---------------------------------------------------------------------------
def get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("EnergyData_Silver_Cleaning_Enrichment") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def load_config(config_path: str = "config/databricks_config.json") -> Dict[str, Any]:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "storage": {
                "paths": {
                    "bronze": "/mnt/lakehouse/bronze/energy_events",
                    "silver": "/mnt/lakehouse/silver/energy_telemetry",
                    "quarantine": "/mnt/lakehouse/quarantine/invalid_events",
                    "checkpoints": {
                        "silver": "/mnt/lakehouse/checkpoints/silver_cleaning",
                        "quarantine": "/mnt/lakehouse/checkpoints/quarantine_routing",
                    },
                },
                "tables": {
                    "bronze": "bronze_energy_events",
                    "silver": "silver_energy_telemetry",
                    "quarantine": "quarantine_invalid_events",
                },
            },
            "streaming": {
                "watermark_duration": "15 minutes",
                "deduplication_keys": ["device_id", "event_time"],
                "trigger_interval": "10 seconds",
            },
            "quality_rules": {
                "voltage_min_v": 100.0,
                "voltage_max_v": 280.0,
                "current_min_a": 0.0,
                "power_factor_min": 0.0,
                "power_factor_max": 1.0,
                "frequency_min_hz": 48.0,
                "frequency_max_hz": 52.0,
            },
        }


# ---------------------------------------------------------------------------
# Transformations Silver
# ---------------------------------------------------------------------------
def parse_and_clean_bronze_to_silver(
    bronze_df,
    watermark_duration: str = "15 minutes",
    quality_rules: Dict[str, float] = None,
):
    """
    Transforme le flux Bronze en DataFrame Silver nettoyé :
      1. Parsing JSON du payload brut
      2. Extraction et aplatissement des colonnes métiers
      3. Typage strict et conversion d'event_time en TIMESTAMP
      4. Définition du Watermark pour les données en retard
      5. Déduplication par (device_id, event_time)
      6. Validation des règles d'intégrité physique (is_valid_reading)
      7. Calcul de la latence d'ingestion (ingestion_latency_sec)
    """
    if quality_rules is None:
        quality_rules = {
            "voltage_min_v": 100.0,
            "voltage_max_v": 280.0,
            "current_min_a": 0.0,
            "power_factor_min": 0.0,
            "power_factor_max": 1.0,
            "frequency_min_hz": 48.0,
            "frequency_max_hz": 52.0,
        }

    # 1. Parsing JSON
    parsed_df = bronze_df.withColumn(
        "parsed",
        from_json(col("raw_payload"), IOT_PAYLOAD_SCHEMA),
    )

    # 2. Aplatissement & Typage
    flattened_df = parsed_df.select(
        col("parsed.device_id").alias("device_id"),
        col("parsed.substation_id").alias("substation_id"),
        col("parsed.location.zone").alias("zone"),
        to_timestamp(col("parsed.timestamp")).alias("event_time"),
        col("parsed.metrics.voltage_v").cast(DoubleType()).alias("voltage_v"),
        col("parsed.metrics.current_a").cast(DoubleType()).alias("current_a"),
        col("parsed.metrics.active_power_kw").cast(DoubleType()).alias("active_power_kw"),
        col("parsed.metrics.reactive_power_kvar").cast(DoubleType()).alias("reactive_power_kvar"),
        col("parsed.metrics.power_factor").cast(DoubleType()).alias("power_factor"),
        col("parsed.metrics.frequency_hz").cast(DoubleType()).alias("frequency_hz"),
        col("parsed.metrics.ambient_temp_c").cast(DoubleType()).alias("ambient_temp_c"),
        col("parsed.status_flag").alias("status_flag"),
        col("ingestion_timestamp"),
    )

    # 3. Filtrage des enregistrements malformés (device_id ou event_time manquant)
    non_null_df = flattened_df.filter(
        col("device_id").isNotNull() & col("event_time").isNotNull()
    )

    # 4. Watermarking
    watermarked_df = non_null_df.withWatermark("event_time", watermark_duration)

    # 5. Déduplication en continu
    deduped_df = watermarked_df.dropDuplicates(["device_id", "event_time"])

    # 6. Règles de validité physique
    validity_condition = (
        (col("voltage_v").between(quality_rules["voltage_min_v"], quality_rules["voltage_max_v"])) &
        (col("current_a") >= quality_rules["current_min_a"]) &
        (col("power_factor").between(quality_rules["power_factor_min"], quality_rules["power_factor_max"])) &
        (col("frequency_hz").between(quality_rules["frequency_min_hz"], quality_rules["frequency_max_hz"])) &
        (col("active_power_kw") >= 0.0)
    )

    validated_df = deduped_df.withColumn(
        "is_valid_reading",
        when(validity_condition, lit(True)).otherwise(lit(False)),
    )

    # 7. Calcul de la latence de traitement
    silver_df = validated_df.withColumn(
        "ingestion_latency_sec",
        (unix_timestamp(col("ingestion_timestamp")) - unix_timestamp(col("event_time"))).cast(DoubleType()),
    )

    return silver_df


def write_silver_delta_stream(
    silver_df,
    table_path: str,
    checkpoint_path: str,
    table_name: str = "silver_energy_telemetry",
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
):
    """
    Écrit le flux Silver nettoyé dans la table Delta Lake.
    """
    writer = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true")

    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)

    logger.info(f"Démarrage du flux Silver vers : {table_path} (Table: {table_name})")
    query = writer.toTable(table_name) if spark_table_exists(table_name) else writer.start(table_path)
    return query


def spark_table_exists(table_name: str) -> bool:
    try:
        spark = SparkSession.getActiveSession()
        return spark is not None and spark.catalog.tableExists(table_name)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------
def main():
    logger.info("=== Démarrage du pipeline de nettoyage Silver (Sprint 3) ===")
    spark = get_spark_session()
    config = load_config()

    bronze_path = config.get("storage", {}).get("paths", {}).get("bronze", "/mnt/lakehouse/bronze/energy_events")
    bronze_table = config.get("storage", {}).get("tables", {}).get("bronze", "bronze_energy_events")
    silver_path = config.get("storage", {}).get("paths", {}).get("silver", "/mnt/lakehouse/silver/energy_telemetry")
    silver_table = config.get("storage", {}).get("tables", {}).get("silver", "silver_energy_telemetry")
    checkpoint_path = config.get("storage", {}).get("paths", {}).get("checkpoints", {}).get("silver", "/mnt/lakehouse/checkpoints/silver_cleaning")

    watermark_duration = config.get("streaming", {}).get("watermark_duration", "15 minutes")
    trigger_interval = config.get("streaming", {}).get("trigger_interval", "10 seconds")
    quality_rules = config.get("quality_rules", {})

    # Lecture du flux Bronze
    logger.info(f"Lecture du flux Delta Bronze depuis : {bronze_path}")
    try:
        bronze_stream = spark.readStream.format("delta").load(bronze_path)
    except Exception as e:
        logger.warning(f"Chemin Bronze inaccessible ({e}). Tentative par nom de table '{bronze_table}'.")
        bronze_stream = spark.readStream.format("delta").table(bronze_table)

    # Transformation & nettoyage
    silver_stream = parse_and_clean_bronze_to_silver(
        bronze_df=bronze_stream,
        watermark_duration=watermark_duration,
        quality_rules=quality_rules,
    )

    # Écriture dans la table Silver
    query = write_silver_delta_stream(
        silver_df=silver_stream,
        table_path=silver_path,
        checkpoint_path=checkpoint_path,
        table_name=silver_table,
        trigger_interval=trigger_interval,
    )

    logger.info(f"Stream Silver actif avec ID : {query.id}")
    return query


if __name__ == "__main__":
    query = main()
    # query.awaitTermination()

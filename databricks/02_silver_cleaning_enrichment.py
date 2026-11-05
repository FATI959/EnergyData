# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ EnergyData Lakehouse — Nettoyage, Déduplication & Couche Silver
# MAGIC **Sprint 3 | Data Platform Engineering**
# MAGIC
# MAGIC Ce notebook consomme la table Delta Bronze, applique :
# MAGIC - Le parsing strict du payload JSON (Contrat d'Interface 1 & 2)
# MAGIC - Le **Watermarking (15 minutes)** pour les événements en retard
# MAGIC - La **déduplication** sur `(device_id, event_time)`
# MAGIC - Les règles de **plausibilité physique** (`is_valid_reading`)
# MAGIC - Le calcul de la **latence d'ingestion** (`ingestion_latency_sec`)

# COMMAND ----------
import json
import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    to_timestamp,
    unix_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("02_silver_cleaning")

# COMMAND ----------
def get_spark_session() -> SparkSession:
    """Récupère ou crée la session Spark."""
    return SparkSession.builder \
        .appName("EnergyData_Silver_Cleaning_Enrichment") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

spark = get_spark_session()

# COMMAND ----------
# Schéma du payload IoT selon Contrat d'Interface 1
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

# COMMAND ----------
def load_config(config_path: str = "config/databricks_config.json") -> dict[str, Any]:
    """Charge la configuration du pipeline Silver."""
    try:
        with open(config_path, encoding="utf-8") as f:
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
                    },
                },
            },
            "streaming": {
                "watermark_duration": "15 minutes",
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

# COMMAND ----------
def parse_and_clean_bronze_to_silver(
    bronze_df,
    watermark_duration: str = "15 minutes",
    quality_rules: dict[str, float] | None = None,
):
    """Transforme le flux Bronze en DataFrame Silver nettoyé."""
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

    # 1. Parsing JSON du payload brut
    parsed_df = bronze_df.withColumn(
        "parsed",
        from_json(col("raw_payload"), IOT_PAYLOAD_SCHEMA),
    )

    # 2. Aplatissement des métriques et localisation
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

    # 3. Filtrage des enregistrements orphelins (clés obligatoires)
    valid_keys_df = flattened_df.filter(
        col("device_id").isNotNull() & col("event_time").isNotNull()
    )

    # 4. Watermarking pour gestion des retards
    watermarked_df = valid_keys_df.withWatermark("event_time", watermark_duration)

    # 5. Déduplication des messages sur (device_id, event_time)
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

    # 7. Calcul de la latence d'ingestion en secondes
    return validated_df.withColumn(
        "ingestion_latency_sec",
        (unix_timestamp(col("ingestion_timestamp")) - unix_timestamp(col("event_time"))).cast(DoubleType()),
    )

# COMMAND ----------
def start_silver_pipeline():
    """Initialise et lance la requête de streaming vers la table Delta Silver."""
    config = load_config()
    storage_cfg = config.get("storage", {})
    bronze_path = storage_cfg.get("paths", {}).get("bronze", "/mnt/lakehouse/bronze/energy_events")
    silver_path = storage_cfg.get("paths", {}).get("silver", "/mnt/lakehouse/silver/energy_telemetry")
    checkpoint_path = storage_cfg.get("paths", {}).get("checkpoints", {}).get("silver", "/mnt/lakehouse/checkpoints/silver_cleaning")

    streaming_cfg = config.get("streaming", {})
    watermark_duration = streaming_cfg.get("watermark_duration", "15 minutes")
    trigger_interval = streaming_cfg.get("trigger_interval", "10 seconds")
    quality_rules = config.get("quality_rules", {})

    logger.info(f"Lecture de la table Delta Bronze depuis : {bronze_path}")
    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    silver_stream = parse_and_clean_bronze_to_silver(
        bronze_df=bronze_stream,
        watermark_duration=watermark_duration,
        quality_rules=quality_rules,
    )

    query = silver_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger_interval) \
        .start(silver_path)

    logger.info(f"Stream Silver actif (Query ID: {query.id}) vers {silver_path}")
    return query

# COMMAND ----------
# Lancement du stream
if __name__ == "__main__" or "spark" in globals():
    silver_query = start_silver_pipeline()

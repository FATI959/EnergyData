# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ EnergyData Lakehouse — Ingestion Streaming Bronze
# MAGIC **Sprint 3 | Data Platform Engineering**
# MAGIC
# MAGIC Ce notebook capture les flux IoT en streaming continu depuis Azure Event Hubs
# MAGIC et les enregistre dans la table Delta Lake **Bronze** avec horodatage et partitionnement.

# COMMAND ----------
import json
import logging
import os
import re
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("01_bronze_ingestion")

# COMMAND ----------
# Récupération ou initialisation de la session Spark
def get_spark_session() -> SparkSession:
    """Récupère la session Spark active ou en crée une pour l'exécution locale."""
    return SparkSession.builder \
        .appName("EnergyData_Bronze_Streaming_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

spark = get_spark_session()

# COMMAND ----------
# Gestion sécurisée des secrets Databricks
def get_secret(scope: str, key: str, fallback_env_var: str = "") -> str:
    """Récupère un secret depuis Databricks Secrets ou variables d'environnement."""
    try:
        # dbutils est injecté nativement dans le notebook Databricks
        return dbutils.secrets.get(scope=scope, key=key)  # noqa: F821
    except Exception:
        return os.getenv(fallback_env_var, "")

# COMMAND ----------
def load_config(config_path: str = "config/databricks_config.json") -> dict[str, Any]:
    """Charge la configuration du projet."""
    try:
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "storage": {
                "paths": {
                    "bronze": "/mnt/lakehouse/bronze/energy_events",
                    "checkpoints": {"bronze": "/mnt/lakehouse/checkpoints/bronze_ingestion"},
                },
                "tables": {"bronze": "bronze_energy_events"},
            },
            "event_hubs": {
                "hub_name": "energy-telemetry-stream",
                "consumer_group": "$Default",
                "trigger_interval": "10 seconds",
            },
            "secret_scope": {
                "scope_name": "keyvault-scope",
                "keys": {"event_hubs_connection_string": "eventhubs-connection-string"},
            },
        }

# COMMAND ----------
def extract_namespace_from_conn_str(conn_str: str) -> str:
    """Extrait le namespace Service Bus depuis la chaîne de connexion."""
    match = re.search(r"Endpoint=sb://([^.]+)\.servicebus\.windows\.net", conn_str)
    return match.group(1) if match else "sb-energy-telemetry"

def create_eventhubs_kafka_stream(
    spark_session: SparkSession,
    conn_str: str,
    event_hub_name: str,
    starting_offsets: str = "latest",
):
    """
    Connecte Spark Structured Streaming au endpoint Kafka natif d'Azure Event Hubs (port 9093).
    Ne requiert aucun JAR propriétaire externe sur Databricks.
    """
    namespace = extract_namespace_from_conn_str(conn_str)
    bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
    jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )

    logger.info(f"Connexion au endpoint Kafka Event Hubs : {bootstrap_servers}")
    return spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", event_hub_name) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", "false") \
        .load()

# COMMAND ----------
def transform_to_bronze_schema(raw_df):
    """Standardise les colonnes d'ingestion vers le schéma Bronze."""
    payload_col = col("value").cast("string") if "value" in raw_df.columns else col("body").cast("string")
    time_col = col("timestamp") if "timestamp" in raw_df.columns else col("enqueuedTime")

    return raw_df.select(
        payload_col.alias("raw_payload"),
        time_col.alias("enqueued_time"),
        current_timestamp().alias("ingestion_timestamp"),
        to_date(current_timestamp()).alias("partition_date"),
        lit("Azure_Event_Hubs").alias("source_system"),
    )

# COMMAND ----------
def start_bronze_pipeline():
    """Initialise et lance la requête de streaming vers la table Delta Bronze."""
    config = load_config()
    scope_cfg = config.get("secret_scope", {})
    scope_name = scope_cfg.get("scope_name", "keyvault-scope")
    secret_key = scope_cfg.get("keys", {}).get("event_hubs_connection_string", "eventhubs-connection-string")

    conn_str = get_secret(scope=scope_name, key=secret_key, fallback_env_var="EVENT_HUBS_CONNECTION_STRING")
    event_hub_name = config.get("event_hubs", {}).get("hub_name", "energy-telemetry-stream")
    trigger_interval = config.get("event_hubs", {}).get("trigger_interval", "10 seconds")

    storage_cfg = config.get("storage", {})
    bronze_path = storage_cfg.get("paths", {}).get("bronze", "/mnt/lakehouse/bronze/energy_events")
    checkpoint_path = storage_cfg.get("paths", {}).get("checkpoints", {}).get("bronze", "/mnt/lakehouse/checkpoints/bronze_ingestion")

    if not conn_str:
        logger.warning("Chaîne de connexion Event Hubs non configurée. Démarrage en mode flux simulé.")
        raw_stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        sample_json = (
            '{"device_id": "METER_SECTOR_A_001", "timestamp": "2026-10-15T14:15:00.000Z", '
            '"substation_id": "SUBSTATION_NORTH_01", "location": {"latitude": 48.85, "longitude": 2.35, "zone": "Zone_A"}, '
            '"metrics": {"voltage_v": 230.0, "current_a": 10.0, "active_power_kw": 2.3, "reactive_power_kvar": 0.2, '
            '"power_factor": 0.98, "frequency_hz": 50.0, "ambient_temp_c": 20.0}, "status_flag": "NORMAL"}'
        )
        raw_stream = raw_stream.select(lit(sample_json).alias("value"), col("timestamp"))
    else:
        raw_stream = create_eventhubs_kafka_stream(
            spark_session=spark,
            conn_str=conn_str,
            event_hub_name=event_hub_name,
        )

    bronze_df = transform_to_bronze_schema(raw_stream)

    query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("partition_date") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger_interval) \
        .start(bronze_path)

    logger.info(f"Stream Bronze actif (Query ID: {query.id}) vers {bronze_path}")
    return query

# COMMAND ----------
# Lancement du stream (dans Databricks, laissez tourner sans bloquer l'interface)
if __name__ == "__main__" or "spark" in globals():
    bronze_query = start_bronze_pipeline()

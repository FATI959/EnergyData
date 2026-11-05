# Databricks notebook source
"""
EnergyData Lakehouse Platform — Ingestion Streaming Bronze
============================================================
Sprint 3 | Membre A — Data Platform Engineer

Pipeline Spark Structured Streaming de capture continue des événements IoT
depuis Azure Event Hubs vers la couche Bronze Delta Lake.

Schéma Bronze généré:
  - raw_payload         : STRING (JSON brut sans altération)
  - enqueued_time       : TIMESTAMP (Horodatage Event Hubs)
  - ingestion_timestamp : TIMESTAMP (Date d'ingestion = current_timestamp())
  - partition_date      : DATE (Partition physique = to_date(ingestion_timestamp))
  - source_system       : STRING ("Azure_Event_Hubs")

Usage:
  Exécuter dans Databricks avec un cluster All-Purpose ou Job Cluster.
"""

import json
import logging
import os
import re
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("01_bronze_ingestion")


# ---------------------------------------------------------------------------
# Initialisation de session Spark
# ---------------------------------------------------------------------------
def get_spark_session() -> SparkSession:
    """Récupère ou initialise la session Spark active avec Delta Lake."""
    return SparkSession.builder \
        .appName("EnergyData_Bronze_Streaming_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


# ---------------------------------------------------------------------------
# Chargement de la configuration
# ---------------------------------------------------------------------------
def load_config(config_path: str = "config/databricks_config.json") -> Dict[str, Any]:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
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


def extract_namespace_from_conn_str(conn_str: str) -> str:
    """Extrait le nom du namespace Event Hubs depuis la chaîne de connexion."""
    match = re.search(r"Endpoint=sb://([^.]+)\.servicebus\.windows\.net", conn_str)
    if match:
        return match.group(1)
    return "sb-energy-telemetry"


# ---------------------------------------------------------------------------
# Construction du Stream Ingestion
# ---------------------------------------------------------------------------
def create_eventhubs_kafka_stream(
    spark: SparkSession,
    conn_str: str,
    event_hub_name: str,
    starting_offsets: str = "latest",
):
    """
    Crée un DataFrame de streaming en utilisant le protocole Kafka exposé nativement
    par Azure Event Hubs (port 9093 avec SASL_SSL/PLAIN).
    Cette méthode est universelle et ne requiert pas de JAR externe tiers sur Databricks.
    """
    namespace = extract_namespace_from_conn_str(conn_str)
    bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
    jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )

    logger.info(f"Connexion au endpoint Kafka Event Hubs : {bootstrap_servers}")

    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", event_hub_name) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", "false") \
        .load()


def transform_to_bronze_schema(raw_df):
    """
    Applique la standardisation du schéma Bronze :
      - raw_payload : payload JSON brut décodé en UTF-8
      - enqueued_time : horodatage de réception Event Hubs
      - ingestion_timestamp : horodatage d'écriture Delta
      - partition_date : date de partitionnement
      - source_system : métadonnée d'origine
    """
    # Détection automatique de la colonne payload (Kafka = 'value', EventHubs Spark = 'body')
    payload_col = col("value").cast("string") if "value" in raw_df.columns else col("body").cast("string")
    time_col = col("timestamp") if "timestamp" in raw_df.columns else col("enqueuedTime")

    return raw_df.select(
        payload_col.alias("raw_payload"),
        time_col.alias("enqueued_time"),
        current_timestamp().alias("ingestion_timestamp"),
        to_date(current_timestamp()).alias("partition_date"),
        lit("Azure_Event_Hubs").alias("source_system"),
    )


def write_bronze_delta_stream(
    bronze_df,
    table_path: str,
    checkpoint_path: str,
    table_name: str = "bronze_energy_events",
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
):
    """
    Écrit le flux Bronze dans Delta Lake avec partitionnement par partition_date.
    """
    writer = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("partition_date") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true")

    if available_now:
        # Mode batch/one-shot idéal pour les tests d'intégration CI/CD
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)

    # Écriture par chemin (ou enregistrement au metastore)
    logger.info(f"Démarrage du flux Bronze vers : {table_path} (Table: {table_name})")
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
    logger.info("=== Démarrage du pipeline de streaming Bronze (Sprint 3) ===")
    spark = get_spark_session()
    config = load_config()

    # Récupération de la chaîne de connexion (Secrets Databricks ou env)
    scope_name = config.get("secret_scope", {}).get("scope_name", "keyvault-scope")
    secret_key = config.get("secret_scope", {}).get("keys", {}).get("event_hubs_connection_string", "eventhubs-connection-string")

    try:
        import dbutils
        conn_str = dbutils.secrets.get(scope=scope_name, key=secret_key)
    except Exception:
        conn_str = os.getenv("EVENT_HUBS_CONNECTION_STRING", "")

    event_hub_name = config.get("event_hubs", {}).get("hub_name", "energy-telemetry-stream")
    trigger_interval = config.get("event_hubs", {}).get("trigger_interval", "10 seconds")
    bronze_path = config.get("storage", {}).get("paths", {}).get("bronze", "/mnt/lakehouse/bronze/energy_events")
    checkpoint_path = config.get("storage", {}).get("paths", {}).get("checkpoints", {}).get("bronze", "/mnt/lakehouse/checkpoints/bronze_ingestion")
    table_name = config.get("storage", {}).get("tables", {}).get("bronze", "bronze_energy_events")

    if not conn_str:
        logger.warning("Aucune chaîne de connexion Event Hubs fournie. Initialisation d'un stream local simulé.")
        # Flux de test local pour démonstration
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
            spark=spark,
            conn_str=conn_str,
            event_hub_name=event_hub_name,
        )

    bronze_stream = transform_to_bronze_schema(raw_stream)
    query = write_bronze_delta_stream(
        bronze_df=bronze_stream,
        table_path=bronze_path,
        checkpoint_path=checkpoint_path,
        table_name=table_name,
        trigger_interval=trigger_interval,
    )

    logger.info(f"Stream Bronze actif avec ID : {query.id}")
    return query


if __name__ == "__main__":
    query = main()
    # Dans Databricks, laisser tourner la requête de streaming
    # query.awaitTermination()

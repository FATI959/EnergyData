# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ EnergyData Lakehouse — Contrôle Data Quality & Table de Quarantaine (DLQ)
# MAGIC **Sprint 4 | Data Platform Engineering**
# MAGIC
# MAGIC Ce notebook implémente le moteur de gouvernance et de qualité des données :
# MAGIC - Validation des **règles d'intégrité physique et électrique** sur la couche Silver
# MAGIC - Aiguillage automatisé des rejets vers la **Dead Letter Queue (Quarantaine)**
# MAGIC - Traçabilité des motifs d'erreur (`quarantine_reason`, `severity`)
# MAGIC - Mesure des **SLAs de qualité** (objectif contractuel : taux de validité $\ge 98\,\%$)

# COMMAND ----------
import json
import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    array_join,
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("03_data_quality")

# COMMAND ----------
# Initialisation ou récupération de la session Spark
def get_spark_session() -> SparkSession:
    """Récupère ou crée la session Spark avec support Delta Lake."""
    return SparkSession.builder \
        .appName("EnergyData_Data_Quality_Quarantine") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

spark = get_spark_session()

# COMMAND ----------
def load_config(config_path: str = "config/databricks_config.json") -> dict[str, Any]:
    """Charge la configuration des règles et des chemins de stockage."""
    try:
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "storage": {
                "paths": {
                    "silver": "/mnt/lakehouse/silver/energy_telemetry",
                    "quarantine": "/mnt/lakehouse/quarantine/invalid_events",
                    "checkpoints": {
                        "quarantine": "/mnt/lakehouse/checkpoints/quarantine_routing",
                    },
                },
                "tables": {
                    "silver": "silver_energy_telemetry",
                    "quarantine": "quarantine_invalid_events",
                },
            },
            "quality_rules": {
                "voltage_min_v": 100.0,
                "voltage_max_v": 280.0,
                "current_min_a": 0.0,
                "current_max_a": 250.0,
                "power_factor_min": 0.0,
                "power_factor_max": 1.0,
                "frequency_min_hz": 48.0,
                "frequency_max_hz": 52.0,
                "sla_target_percent": 98.0,
            },
        }

# COMMAND ----------
# Règles de validation détaillées & tag des motifs de panne
def apply_quality_checks_and_tagging(silver_df: DataFrame, quality_rules: dict[str, float]) -> DataFrame:
    """
    Applique le contrôle d'intégrité strict et génère les flags d'erreur précis :
      - VOLTAGE_OUT_OF_BOUNDS (surtension > 280V ou sous-tension < 100V)
      - FREQUENCY_OUT_OF_BOUNDS (fréquence réseau != 50Hz +/- 2Hz)
      - INVALID_POWER_FACTOR (facteur hors [0.0, 1.0])
      - NEGATIVE_CURRENT / CURRENT_EXCEEDED (courant négatif ou dépassement disjoncteur)
      - NEGATIVE_ACTIVE_POWER (puissance consommateur < 0)
    """
    v_min = quality_rules.get("voltage_min_v", 100.0)
    v_max = quality_rules.get("voltage_max_v", 280.0)
    c_min = quality_rules.get("current_min_a", 0.0)
    c_max = quality_rules.get("current_max_a", 250.0)
    pf_min = quality_rules.get("power_factor_min", 0.0)
    pf_max = quality_rules.get("power_factor_max", 1.0)
    f_min = quality_rules.get("frequency_min_hz", 48.0)
    f_max = quality_rules.get("frequency_max_hz", 52.0)

    # Détection des motifs d'invalidation
    df_with_reasons = silver_df.withColumn(
        "err_voltage",
        when((col("voltage_v") < v_min) | (col("voltage_v") > v_max), lit("VOLTAGE_OUT_OF_BOUNDS")).otherwise(None),
    ).withColumn(
        "err_frequency",
        when((col("frequency_hz") < f_min) | (col("frequency_hz") > f_max), lit("FREQUENCY_OUT_OF_BOUNDS")).otherwise(None),
    ).withColumn(
        "err_pf",
        when((col("power_factor") < pf_min) | (col("power_factor") > pf_max), lit("INVALID_POWER_FACTOR")).otherwise(None),
    ).withColumn(
        "err_current",
        when((col("current_a") < c_min) | (col("current_a") > c_max), lit("INVALID_CURRENT")).otherwise(None),
    ).withColumn(
        "err_power",
        when(col("active_power_kw") < 0.0, lit("NEGATIVE_ACTIVE_POWER")).otherwise(None),
    )

    # Concaténation de la liste des erreurs dans une seule chaîne normalisée
    all_reasons = array("err_voltage", "err_frequency", "err_pf", "err_current", "err_power")
    clean_reasons = array_compact(all_reasons)

    evaluated_df = df_with_reasons.withColumn(
        "quarantine_reason",
        when(col("is_valid_reading") == lit(False), array_join(clean_reasons, "; ")).otherwise(None),
    ).withColumn(
        "severity_level",
        when(col("err_voltage").isNotNull() | col("err_frequency").isNotNull(), lit("CRITICAL"))
        .when(col("is_valid_reading") == lit(False), lit("WARNING"))
        .otherwise(lit("PASSED")),
    ).withColumn(
        "validation_timestamp",
        current_timestamp(),
    ).drop("err_voltage", "err_frequency", "err_pf", "err_current", "err_power")

    return evaluated_df

# COMMAND ----------
def extract_quarantine_records(evaluated_df: DataFrame) -> DataFrame:
    """Isole exclusivement les enregistrements non conformes pour la Dead Letter Queue."""
    return evaluated_df.filter(
        (col("is_valid_reading") == lit(False)) | (col("quarantine_reason").isNotNull())
    ).select(
        col("device_id"),
        col("substation_id"),
        col("zone"),
        col("event_time"),
        col("voltage_v"),
        col("current_a"),
        col("active_power_kw"),
        col("power_factor"),
        col("frequency_hz"),
        col("quarantine_reason"),
        col("severity_level"),
        col("validation_timestamp").alias("quarantined_at"),
    )

# COMMAND ----------
def calculate_sla_metrics(evaluated_df: DataFrame, sla_target: float = 98.0) -> dict[str, Any]:
    """
    Calcule le rapport de conformité global et vérifie le respect du SLA (≥ 98 %).
    Fonctionne aussi bien sur un DataFrame batch ou sur un micro-batch de streaming.
    """
    metrics = evaluated_df.agg(
        spark_sum(when(col("is_valid_reading") == lit(True), 1).otherwise(0)).alias("valid_count"),
        spark_sum(when(col("is_valid_reading") == lit(False), 1).otherwise(0)).alias("invalid_count"),
    ).collect()

    valid = metrics[0]["valid_count"] or 0
    invalid = metrics[0]["invalid_count"] or 0
    total = valid + invalid

    compliance_pct = round((valid / total * 100.0), 2) if total > 0 else 100.0
    sla_respected = compliance_pct >= sla_target

    report = {
        "total_readings": total,
        "valid_readings": valid,
        "quarantined_readings": invalid,
        "compliance_percentage": compliance_pct,
        "sla_target_percentage": sla_target,
        "sla_status": "CONFORME" if sla_respected else "VIOLATION_SLA",
    }
    logger.info(
        f"Rapport Data Quality — Taux: {compliance_pct}% (Objectif: {sla_target}%) — Statut: {report['sla_status']}"
    )
    return report

# COMMAND ----------
def start_quarantine_streaming_pipeline():
    """
    Pipeline Spark Structured Streaming consommant la table Silver et écrivant
    automatiquement les rejets dans la table Delta de quarantaine.
    """
    config = load_config()
    storage_cfg = config.get("storage", {})
    silver_path = storage_cfg.get("paths", {}).get("silver", "/mnt/lakehouse/silver/energy_telemetry")
    quarantine_path = storage_cfg.get("paths", {}).get("quarantine", "/mnt/lakehouse/quarantine/invalid_events")
    quarantine_chk = storage_cfg.get("paths", {}).get("checkpoints", {}).get(
        "quarantine", "/mnt/lakehouse/checkpoints/quarantine_routing"
    )
    quality_rules = config.get("quality_rules", {})

    logger.info(f"Écoute du flux Silver depuis : {silver_path}")
    silver_stream = spark.readStream.format("delta").load(silver_path)

    # Évaluation de la qualité
    evaluated_stream = apply_quality_checks_and_tagging(silver_stream, quality_rules)

    # Filtrage DLQ
    quarantine_stream = extract_quarantine_records(evaluated_stream)

    # Écriture en Delta Quarantaine
    query = quarantine_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", quarantine_chk) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start(quarantine_path)

    logger.info(f"Stream de Quarantaine actif (Query ID: {query.id}) vers {quarantine_path}")
    return query

# COMMAND ----------
# Lancement de la surveillance Quarantaine
if __name__ == "__main__" or "spark" in globals():
    quarantine_query = start_quarantine_streaming_pipeline()

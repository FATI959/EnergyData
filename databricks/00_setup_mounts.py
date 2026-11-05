# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ EnergyData Lakehouse — Configuration du Stockage ADLS Gen2
# MAGIC **Sprint 3 | Data Platform Engineering**
# MAGIC
# MAGIC Ce notebook configure le point de montage Azure Data Lake Storage Gen2 (ADLS Gen2)
# MAGIC sur DBFS (`/mnt/lakehouse`) et initialise les répertoires Medallion (Bronze, Silver, Gold).

# COMMAND ----------
import json
import logging
import os
from typing import Any

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("00_setup_mounts")

# COMMAND ----------
# Gestion robuste de dbutils (local vs Databricks)
# Évite toute erreur 'dbutils is not defined' dans les éditeurs locaux
dbutils = globals().get("dbutils", None)

if dbutils is None:
    class MockDBUtilsFS:
        def mounts(self):
            return []
        def mount(self, source, mount_point, extra_configs=None):
            logger.info(f"[SIMULATION] Montage de {source} vers {mount_point}")
        def unmount(self, mount_point):
            logger.info(f"[SIMULATION] Démontage de {mount_point}")
        def mkdirs(self, path):
            logger.info(f"[SIMULATION] Création du dossier {path}")
        def ls(self, path):
            return []

    class MockDBUtilsSecrets:
        def get(self, scope, key):
            env_map = {
                "adls-storage-key": os.getenv("AZURE_STORAGE_KEY", "mock-storage-key"),
                "eventhubs-connection-string": os.getenv("EVENT_HUBS_CONNECTION_STRING", "mock-eh-conn"),
            }
            return env_map.get(key, f"mock-secret-{key}")

    class MockDBUtils:
        fs = MockDBUtilsFS()
        secrets = MockDBUtilsSecrets()

    dbutils = MockDBUtils()
    logger.info("dbutils émulé localement (hors cluster Databricks).")

# COMMAND ----------
def load_config(config_path: str = "config/databricks_config.json") -> dict[str, Any]:
    """Charge la configuration du Lakehouse."""
    try:
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "storage": {
                "account_name": "stenergydatalakehouse",
                "container_name": "lakehouse",
                "mount_point": "/mnt/lakehouse",
            },
            "secret_scope": {
                "scope_name": "keyvault-scope",
                "keys": {"adls_storage_key": "adls-storage-key"},
            },
        }

# COMMAND ----------
def mount_adls_gen2(
    storage_account: str,
    container_name: str,
    mount_point: str,
    secret_scope: str = "keyvault-scope",
    secret_key_name: str = "adls-storage-key",
    use_service_principal: bool = False,
    client_id: str | None = None,
    client_secret: str | None = None,
    tenant_id: str | None = None,
) -> bool:
    """Monte le stockage ADLS Gen2 sur DBFS de manière idempotente."""
    source_uri = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net"

    try:
        current_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
        if mount_point in current_mounts:
            logger.info(f"Le point de montage '{mount_point}' existe déjà.")
            return True

        logger.info(f"Montage de '{source_uri}' sur '{mount_point}'...")
        if use_service_principal and client_id and client_secret and tenant_id:
            extra_configs = {
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            }
            abfss_uri = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
            dbutils.fs.mount(source=abfss_uri, mount_point=mount_point, extra_configs=extra_configs)
        else:
            storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
            extra_configs = {
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net": storage_key
            }
            dbutils.fs.mount(source=source_uri, mount_point=mount_point, extra_configs=extra_configs)

        logger.info(f"Montage réussi sur {mount_point} !")
        return True

    except Exception as e:
        logger.warning(f"Montage non complété ({e}). Mode simulation DBFS conservé.")
        return False

# COMMAND ----------
def initialize_lakehouse_directories(mount_point: str) -> None:
    """Crée l'arborescence Medallion sous le point de montage."""
    directories = [
        f"{mount_point}/bronze/energy_events",
        f"{mount_point}/silver/energy_telemetry",
        f"{mount_point}/gold/energy_5min_aggregates",
        f"{mount_point}/gold/detected_anomalies",
        f"{mount_point}/gold/load_forecasts",
        f"{mount_point}/quarantine/invalid_events",
        f"{mount_point}/checkpoints/bronze_ingestion",
        f"{mount_point}/checkpoints/silver_cleaning",
        f"{mount_point}/checkpoints/gold_aggregations",
        f"{mount_point}/checkpoints/quarantine_routing",
    ]
    for d in directories:
        try:
            dbutils.fs.mkdirs(d)
        except Exception as e:
            logger.debug(f"Info création dossier {d}: {e}")

# COMMAND ----------
# Exécution du montage
config = load_config()
storage_cfg = config.get("storage", {})
scope_cfg = config.get("secret_scope", {})

account_name = storage_cfg.get("account_name", "stenergydatalakehouse")
container_name = storage_cfg.get("container_name", "lakehouse")
mount_point = storage_cfg.get("mount_point", "/mnt/lakehouse")
scope_name = scope_cfg.get("scope_name", "keyvault-scope")
secret_key = scope_cfg.get("keys", {}).get("adls_storage_key", "adls-storage-key")

mounted = mount_adls_gen2(
    storage_account=account_name,
    container_name=container_name,
    mount_point=mount_point,
    secret_scope=scope_name,
    secret_key_name=secret_key,
)

if mounted:
    initialize_lakehouse_directories(mount_point)
    logger.info("Stockage Medallion prêt pour l'ingestion.")

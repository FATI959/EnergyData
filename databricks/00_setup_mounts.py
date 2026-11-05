# Databricks notebook source
"""
EnergyData Lakehouse Platform — ADLS Gen2 & Key Vault Setup
=============================================================
Sprint 3 | Membre A — Data Platform Engineer

Configure le point de montage Azure Data Lake Storage Gen2 (ADLS Gen2)
dans Databricks (DBFS) et initialise l'arborescence Medallion (Bronze, Silver, Gold).
Utilise Databricks Secrets (connecté à Azure Key Vault) pour la sécurité.

Usage dans Databricks:
    Exécuter ce notebook une fois lors de l'initialisation du cluster.
"""

import json
import logging
from typing import Dict, Any

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("00_setup_mounts")

# ---------------------------------------------------------------------------
# Fallback helper pour exécution hors Databricks (tests locaux / CI)
# ---------------------------------------------------------------------------
try:
    # Dans Databricks runtime, dbutils et spark sont injectés automatiquement
    dbutils_available = "dbutils" in globals()
except NameError:
    dbutils_available = False

if not dbutils_available:
    class MockDBUtilsFS:
        def mounts(self):
            return []
        def mount(self, source, mount_point, extra_configs=None):
            logger.info(f"[MOCK] Mounted {source} to {mount_point}")
        def unmount(self, mount_point):
            logger.info(f"[MOCK] Unmounted {mount_point}")
        def mkdirs(self, path):
            logger.info(f"[MOCK] Created directory {path}")
        def ls(self, path):
            return []

    class MockDBUtilsSecrets:
        def get(self, scope, key):
            import os
            # Fallback sur variable d'environnement locale si disponible
            env_map = {
                "adls-storage-key": os.getenv("AZURE_STORAGE_KEY", "mock-storage-key"),
                "eventhubs-connection-string": os.getenv("EVENT_HUBS_CONNECTION_STRING", "mock-eh-conn"),
            }
            return env_map.get(key, f"mock-secret-{key}")

    class MockDBUtils:
        fs = MockDBUtilsFS()
        secrets = MockDBUtilsSecrets()

    dbutils = MockDBUtils()
    logger.info("dbutils non détecté — mode simulation local activé.")


# ---------------------------------------------------------------------------
# Fonctions de configuration & montage
# ---------------------------------------------------------------------------
def load_config(config_path: str = "config/databricks_config.json") -> Dict[str, Any]:
    """Charge la configuration Databricks depuis le JSON du projet."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Fichier de configuration {config_path} non trouvé. Utilisation des valeurs par défaut.")
        return {
            "storage": {
                "account_name": "stenergydatalakehouse",
                "container_name": "lakehouse",
                "mount_point": "/mnt/lakehouse",
            },
            "secret_scope": {
                "scope_name": "keyvault-scope",
                "keys": {
                    "adls_storage_key": "adls-storage-key",
                }
            }
        }


def mount_adls_gen2(
    storage_account: str,
    container_name: str,
    mount_point: str,
    secret_scope: str = "keyvault-scope",
    secret_key_name: str = "adls-storage-key",
    use_service_principal: bool = False,
    client_id: str = None,
    client_secret: str = None,
    tenant_id: str = None,
) -> bool:
    """
    Monte un conteneur ADLS Gen2 sur DBFS si ce n'est pas déjà fait.

    Supporte :
      1. Clé de compte de stockage via Secret Scope (méthode rapide)
      2. Service Principal OAuth (méthode de production recommandée)
    """
    source_uri = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net"

    # Vérifier si déjà monté
    current_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    if mount_point in current_mounts:
        logger.info(f"Le point de montage '{mount_point}' existe déjà.")
        return True

    logger.info(f"Montage de '{source_uri}' sur '{mount_point}'...")

    try:
        if use_service_principal and client_id and client_secret and tenant_id:
            extra_configs = {
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            }
            # URI pour driver ABFSS natif
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
        logger.error(f"Échec du montage ADLS : {str(e)}")
        # Ne pas bloquer si exécuté en sandbox
        return False


def initialize_lakehouse_directories(mount_point: str) -> None:
    """
    Crée l'arborescence des répertoires Medallion (Bronze, Silver, Gold, Quarantaine, Checkpoints).
    """
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
            logger.info(f"Dossier initialisé : {d}")
        except Exception as e:
            logger.warning(f"Impossible de créer le dossier {d} : {e}")


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------
def main():
    logger.info("=== Initialisation du Montage Lakehouse ADLS Gen2 ===")
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
        logger.info("=== Configuration du stockage Lakehouse terminée avec succès ===")
    else:
        logger.warning("Montage non complété. Vérifiez vos identifiants Azure / Key Vault.")


if __name__ == "__main__":
    main()

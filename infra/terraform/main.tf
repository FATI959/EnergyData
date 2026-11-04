# ==============================================================================
# EnergyData Lakehouse — Infrastructure as Code (Terraform)
# Sprint 2 | Membre A — Data Platform Engineer
# ==============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# ------------------------------------------------------------------------------
# 1. Groupe de Ressources
# ------------------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location

  tags = var.tags
}

# ------------------------------------------------------------------------------
# 2. Azure Event Hubs (Namespace & Topic)
# ------------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "eh_namespace" {
  name                = var.eventhub_namespace_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = var.eventhub_sku
  capacity            = var.eventhub_capacity

  auto_inflate_enabled     = true
  maximum_throughput_units = 5

  tags = var.tags
}

resource "azurerm_eventhub" "telemetry_hub" {
  name                = var.eventhub_name
  namespace_name      = azurerm_eventhub_namespace.eh_namespace.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = var.eventhub_partition_count
  message_retention   = 1 # 1 jour de rétention pour le streaming IoT
}

# Shared Access Policy pour le simulateur/producteur IoT (Send)
resource "azurerm_eventhub_authorization_rule" "producer_rule" {
  name                = "iot-producer-policy"
  namespace_name      = azurerm_eventhub_namespace.eh_namespace.name
  eventhub_name       = azurerm_eventhub.telemetry_hub.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = false
  send                = true
  manage              = false
}

# Shared Access Policy pour Spark Structured Streaming (Listen)
resource "azurerm_eventhub_authorization_rule" "spark_reader_rule" {
  name                = "spark-stream-reader-policy"
  namespace_name      = azurerm_eventhub_namespace.eh_namespace.name
  eventhub_name       = azurerm_eventhub.telemetry_hub.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = false
  manage              = false
}

# Consumer Group dédié pour Apache Spark / Databricks
resource "azurerm_eventhub_consumer_group" "spark_consumer_group" {
  name                = "spark-streaming-cg"
  namespace_name      = azurerm_eventhub_namespace.eh_namespace.name
  eventhub_name       = azurerm_eventhub.telemetry_hub.name
  resource_group_name = azurerm_resource_group.rg.name
}

# ------------------------------------------------------------------------------
# 3. Azure Data Lake Storage Gen2 (Préparation Sprint 3 Medallion)
# ------------------------------------------------------------------------------
resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Activation Hierarchical Namespace (ADLS Gen2)

  tags = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse_container" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.adls.id
}

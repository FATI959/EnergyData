# ==============================================================================
# EnergyData Lakehouse — Terraform Outputs
# Sprint 2 | Membre A — Data Platform Engineer
# ==============================================================================

output "resource_group_name" {
  value       = azurerm_resource_group.rg.name
  description = "Nom du Resource Group"
}

output "eventhub_name" {
  value       = azurerm_eventhub.telemetry_hub.name
  description = "Nom du Hub de télémétrie"
}

output "producer_primary_connection_string" {
  value       = azurerm_eventhub_authorization_rule.producer_rule.primary_connection_string
  sensitive   = true
  description = "Chaîne de connexion SAS pour le producteur IoT (à copier dans .env)"
}

output "spark_reader_primary_connection_string" {
  value       = azurerm_eventhub_authorization_rule.spark_reader_rule.primary_connection_string
  sensitive   = true
  description = "Chaîne de connexion SAS pour Spark Streaming (Sprint 3)"
}

output "adls_primary_dfs_endpoint" {
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
  description = "Point de terminaison DFS pour le montage Databricks"
}

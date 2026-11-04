# ==============================================================================
# EnergyData Lakehouse — Terraform Variables
# Sprint 2 | Membre A — Data Platform Engineer
# ==============================================================================

variable "location" {
  type        = string
  default     = "francecentral"
  description = "Région Azure pour le déploiement des ressources"
}

variable "resource_group_name" {
  type        = string
  default     = "rg-energydata-lakehouse"
  description = "Nom du Resource Group principal"
}

variable "eventhub_namespace_name" {
  type        = string
  default     = "eh-energydata-stream-ns"
  description = "Nom unique du Namespace Azure Event Hubs"
}

variable "eventhub_name" {
  type        = string
  default     = "energy-telemetry-stream"
  description = "Nom du Hub / Topic IoT"
}

variable "eventhub_sku" {
  type        = string
  default     = "Standard"
  description = "Tier du Namespace (Standard requis pour multi-partitions et consumer groups)"
}

variable "eventhub_capacity" {
  type        = number
  default     = 2
  description = "Throughput Units (TUs). 2 TUs = 2 MB/s ingress (~2000 msgs/s)"
}

variable "eventhub_partition_count" {
  type        = number
  default     = 4
  description = "Nombre de partitions physiques (recommandé: 4 pour Spark)"
}

variable "storage_account_name" {
  type        = string
  default     = "stenergydatalake2026"
  description = "Nom du compte ADLS Gen2 (3-24 caractères alphanumériques minuscules)"
}

variable "tags" {
  type = map(string)
  default = {
    Environment = "Development"
    Project     = "EnergyData-Lakehouse"
    Team        = "DataPlatform"
    ManagedBy   = "Terraform"
  }
}

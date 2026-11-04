# ⚡ Real-Time Energy Data Lakehouse & Predictive Analytics Platform
## 📋 Plan d'Implémentation Détaillé (Projet PFE & Portfolio CV)

---

## 1. Vue d'Ensemble & Objectifs

Ce projet a pour objectif de concevoir et déployer de bout en bout une plateforme moderne de **Lakehouse temps réel** dédiée aux données télémétriques de compteurs intelligents (*Smart Meters* et sous-stations électriques).

### 🎯 Objectifs Clés
- **Ingestion Temps Réel Scalable** : Capture continue d'événements IoT haute fréquence via **Azure Event Hubs**.
- **Architecture Medallion (Bronze / Silver / Gold)** : Traitement streaming structuré avec **Apache Spark** et **Delta Lake** sur **Databricks**.
- **Gouvernance & Qualité (Data Quality)** : Contrôle automatisé des flux (schéma, validité physique, complétude) et aiguillage en quarantaine (*Dead Letter Queue*).
- **Intelligence Prédictive & MLOps** :
  - Détection d'anomalies en temps réel (chutes de tension, pics anormaux, compteurs défaillants).
  - Prévision de charge énergétique à court/moyen terme (H+24, J+7) traquée avec **MLflow**.
- **Restitution Métier** : Tableaux de bord opérationnels et analytiques connectés en **DirectQuery** sous **Power BI**.

---

## 2. Structure Recommandée du Repository

```text
EnergyData/
├── .github/
│   └── workflows/
│       ├── ci-pipeline.yml               # Linting, tests unitaires et intégration
│       └── deploy-databricks.yml         # Déploiement des notebooks / assets Databricks
├── config/
│   ├── databricks_config.json            # Paramètres clusters, chemins DBFS/ADLS
│   └── simulation_config.yaml            # Paramètres des capteurs IoT (fréquence, nombre, bruit)
├── data_simulation/
│   ├── __init__.py
│   ├── generator.py                      # Moteur de génération des séries temporelles énergétiques
│   ├── anomaly_injector.py               # Injection contrôlée de pannes et comportements anormaux
│   ├── producer_eventhubs.py             # Client d'envoi vers Azure Event Hubs (Kafka/AMQP)
│   └── weather_enricher.py               # Connecteur API météo externe (Open-Meteo)
├── databricks/
│   ├── 00_setup_mounts.py                # Configuration montage ADLS Gen2 & Secrets Key Vault
│   ├── 01_bronze_streaming_ingestion.py  # Ingestion brute Event Hubs -> Delta Bronze
│   ├── 02_silver_cleaning_enrichment.py  # Parsing, déduplication, watermark, quarantaine
│   ├── 03_data_quality_checks.py         # Règles d'intégrité (Great Expectations / DLT)
│   ├── 04_gold_aggregations.py           # Vues métiers, fenêtrages 5min/1h, KPIs
│   └── 05_ml_feature_store.py            # Extraction et préparation des features pour le ML
├── ml_engine/
│   ├── anomaly_detection/
│   │   ├── train_detector.py             # Entraînement Isolation Forest / Autoencoder
│   │   └── stream_scoring.py             # UDF Spark pour scoring d'anomalies temps réel
│   ├── forecasting/
│   │   ├── train_forecaster.py           # Entraînement Prophet / LightGBM / XGBoost
│   │   └── evaluate_mlflow.py            # Log des métriques (RMSE, MAE, MAPE) dans MLflow
│   └── model_registry/                   # Scripts de transition de modèle (Staging -> Production)
├── powerbi/
│   ├── templates/                        # Fichiers .pbit ou documentation du modèle de données
│   └── dax_measures.md                   # Liste commentée des mesures DAX clés
├── infra/
│   └── terraform/                        # IaC optionnelle (Event Hubs, ADLS, Key Vault, Resource Group)
│       ├── main.tf
│       └── variables.tf
├── tests/
│   ├── test_generator.py                 # Tests unitaires du simulateur
│   └── test_spark_transformations.py     # Tests des transformations logiques Spark
├── .env.example                          # Modèle des variables d'environnement (clés secrètes)
├── .gitignore
├── README.md                             # Documentation d'envergure portfolio
└── requirements.txt                      # Dépendances Python
```

---

## 3. Matrice des Données & Schémas

### 📡 Charge Utile Brute IoT (JSON - Event Hubs)
```json
{
  "device_id": "METER_SECTOR_A_042",
  "timestamp": "2026-10-15T14:15:00.000Z",
  "substation_id": "SUBSTATION_NORTH_02",
  "location": {
    "latitude": 48.8566,
    "longitude": 2.3522,
    "zone": "Industrial_Park"
  },
  "metrics": {
    "voltage_v": 231.4,
    "current_a": 15.8,
    "active_power_kw": 3.65,
    "reactive_power_kvar": 0.42,
    "power_factor": 0.99,
    "frequency_hz": 50.02,
    "ambient_temp_c": 19.5
  },
  "status_flag": "NORMAL"
}
```

### 🧱 Schéma Bronze (Table Delta brute)
- `raw_payload` (STRING) : JSON brut tel que reçu d'Event Hubs.
- `enqueued_time` (TIMESTAMP) : Horodatage d'arrivée dans Event Hubs.
- `ingestion_timestamp` (TIMESTAMP) : Date d'écriture dans Delta Lake (`current_timestamp()`).
- `partition_date` (DATE) : Partitionnement physique (`to_date(ingestion_timestamp)`).

### 🥈 Schéma Silver (Table Delta propre & enrichie)
- `device_id` (STRING), `substation_id` (STRING), `zone` (STRING)
- `event_time` (TIMESTAMP - indexé avec Watermarking de 10-15 min)
- `voltage_v` (DOUBLE), `current_a` (DOUBLE), `active_power_kw` (DOUBLE)
- `power_factor` (DOUBLE), `frequency_hz` (DOUBLE), `ambient_temp_c` (DOUBLE)
- `is_valid_reading` (BOOLEAN) : Flag validé par les règles d'intégrité
- `is_duplicate` (BOOLEAN)
- `ingestion_latency_sec` (DOUBLE) : `unix_timestamp(ingestion_timestamp) - unix_timestamp(event_time)`

### 🥇 Schémas Gold (Tables orientées analyse & BI)
1. **`gold_energy_5min_aggregates`** :
   - Clés : `substation_id`, `zone`, `window_start`, `window_end`.
   - Agrégats : `avg_active_power_kw`, `peak_active_power_kw`, `total_energy_kwh`, `avg_power_factor`, `voltage_min`, `voltage_max`.
2. **`gold_detected_anomalies`** :
   - Clés : `anomaly_id`, `device_id`, `timestamp`, `anomaly_type` (ex: *Overvoltage*, *PowerFactorDrop*, *AbruptDisconnection*), `severity` (Low, Medium, Critical), `z_score`.
3. **`gold_load_forecasts`** :
   - Clés : `forecast_id`, `substation_id`, `target_timestamp`, `predicted_active_power_kw`, `lower_bound_kw`, `upper_bound_kw`, `model_version`.

---

## 4. Feuille de Route d'Exécution par Sprints

| Sprint | Étape Clé | Livrables Concrets |
| :--- | :--- | :--- |
| **Sprint 1** | **Environnement & Générateur IoT** | - Projet initialisé & environnement virtuel configuré.<br>- Script de simulation multi-capteurs réaliste avec anomalies.<br>- Export local & validation des payloads JSON. |
| **Sprint 2** | **Ingestion Azure Event Hubs** | - Déploiement Azure Event Hubs (Standard).<br>- Script producteur streaming sécurisé via connection string.<br>- Test de montée en charge (500 msg/sec). |
| **Sprint 3** | **Couches Bronze & Silver (Databricks)** | - Notebook Databricks Spark Structured Streaming connectant Event Hubs.<br>- Ingestion vers Delta Lake Bronze avec checkpointing.<br>- Nettoyage, typage strict, watermarking & déduplication vers Silver. |
| **Sprint 4** | **Data Quality & Quarantaine** | - Intégration de tests d'intégrité (règles de validité tension/courant).<br>- Implémentation de la table de Quarantaine (*Dead Letter Queue*). |
| **Sprint 5** | **Couche Gold & Métriques Métier** | - Agrégations en streaming sur fenêtres glissantes (5 min, 1h).<br>- Tables dimensionnelles et de faits optimisées pour requêtes analytiques. |
| **Sprint 6** | **ML Engine : Anomalies & Prédictions** | - Modèle de détection d'anomalies en micro-batch (Isolation Forest).<br>- Modèle de forecasting de consommation (Prophet ou XGBoost).<br>- Suivi des expérimentations et versioning dans **MLflow**. |
| **Sprint 7** | **Dashboard Power BI & Restitution** | - Connexion DirectQuery sur Databricks SQL.<br>- 3 pages de dashboard : Opérations temps réel, Analyse historique, Prédictions.<br>- Mesures DAX avancées documentées. |
| **Sprint 8** | **Industrialisation, CI/CD & Rapport PFE** | - Pipeline CI/CD GitHub Actions (tests unitaires & lint).<br>- README d'excellence pour GitHub avec captures et schémas.<br>- Rédaction des chapitres techniques du rapport de PFE. |

---

## 5. Comment ce Projet Valorise votre CV & PFE

1. **Compétences Data Engineering très recherchées** :
   - Maîtrise de l'écosystème moderne : **Databricks, Spark Structured Streaming, Delta Lake, Event Hubs**.
   - Implémentation concrète de l'**Architecture Medallion**, du **Watermarking** et du **Late-arriving data handling**.
2. **Double compétence Data Engineering + MLOps** :
   - Intégration du cycle de vie du Machine Learning avec **MLflow** sur données réelles de capteurs.
3. **Data Governance & Data Quality** :
   - Mise en avant de la gestion d'erreurs (quarantaine, validation automatisée), un sujet crucial en entreprise.
4. **Impact Métier Visible** :
   - Présentation finale sous **Power BI** permettant à un recruteur ou au jury de PFE de voir immédiatement l'application concrète d'aide à la décision.

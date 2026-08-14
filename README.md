# Vélo'v Real-Time Data Pipeline & Monitoring
Ce projet implémente un pipeline de streaming de données en temps réel pour le réseau de vélos en libre-service Vélo'v (Grand Lyon).

Le système collecte les métriques des stations via l'API ouverte du Grand Lyon, détecte les anomalies en temps réel (stations vides, saturées ou à faible stock) grâce à Apache Kafka, enregistre les données spatiales dans PostgreSQL / PostGIS, et visualises les alertes sur un dashboard Grafana.

# Architecture du Pipeline
```
[ API Grand Lyon ]
       │
       ▼
[ Producer Python ] ──────► Kafka Topic: "velov-raw-status"
                                   │
                                   ▼
                        [ Stream Processor Python ] 
                        (Règles métier & Détection)
                                   │
                                   ▼
                        Kafka Topic: "velov-station-alerts"
                                   │
                                   ▼
                        [ Consumer DB Python ]
                                   │
                                   ▼
                       [ PostgreSQL / PostGIS ]
                                   │
                                   ▼
                         [ Grafana Dashboard ]
```
# Structure du Projet

```
kafka-velov-pipeline/
├── .github/
│   └── workflows/
│       └── tests.yml            # Pipeline de CI GitHub Actions (Flake8 & Pytest)
├── config/
│   ├── postgres/
│   │   └── init.sql             # Initialisation PostGIS & table d'alertes
│   │   └── postgres_init.sql    # Script d'initialisation PostgreSQL
│   └── grafana/
│       ├── datasources/
│       │   └── postgres.yml     # Provisioning automatique de la source PostgreSQL
│       └── dashboards/
│           ├── dashboards.yml   # Configuration du tableau de bord
│           └── velov_dashboard.json
├── src/
│   ├── producer.py              # Ingestion API -> Kafka (velov-raw-status)
│   ├── processor.py             # Stream processing (règles d'alertes)
│   └── consumer_db.py           # Ingestion Kafka -> PostgreSQL/PostGIS
├── tests/
│   └── test_processor.py        # Tests unitaires Pytest pour le Stream Processor
├── .gitignore
├── docker-compose.yml           # Infrastructure Kafka, PostGIS & Grafana
│
├── README.md
└── requirements.txt             # Dépendances Python
```

# Guide d'Installation et Test
## Cloner le dépôt et configurer l'environnement Python
```bash
# Cloner le projet
git clone https://github.com/votre-compte/kafka-velov-pipeline.git
cd kafka-velov-pipeline

# Créer un environnement virtuel propre
python -m venv .venv

# Activer l'environnement virtuel
# Sous Windows (PowerShell) :
.\.venv\Scripts\Activate.ps1
# Sous Linux/macOS :
source .venv/bin/activate

# Mettre à jour pip et installer les dépendances
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

## Démarrer l'infrastructure
```bash
docker-compose up -d
```
Note : La base de données PostgreSQL sera automatiquement initialisée avec l'extension PostGIS et la table velov_station_alerts grâce au fichier config/postgres/init.sql.

```bash
docker compose ps
```
## Exécuter le Pipeline de Données
Ouvrez 3 terminaux distincts (en vous assurant que l'environnement .venv est activé dans chacun) :

### Terminal 1 : Démarrer le Producer
Collecte les données de l'API et publie sur le topic velov-raw-status.
```bash
python src/producer.py
```

### Terminal 2 : Démarrer le Processor
Analyse les données brutes, applique les règles métier et génère les alertes vers velov-station-alerts.
```bash
python src/processor.py
```

### Terminal 3 : Démarrer le Consumer
Lit les alertes publiées et les persiste dans PostgreSQL/PostGIS.
```bash
python src/consumer_db.py
```
## Visualiser les Alertes dans Grafana
* Rendez-vous sur http://localhost:3000 dans votre navigateur.
* Connectez-vous avec les identifiants par défaut :
  * Identifiant : admin
  * Mot de passe : admin
* Allez dans Dashboards $\rightarrow$ Vélo'v Real-Time Monitoring.
* La source de données PostgreSQL et la carte géospatiale des alertes sont configurées et opérationnelles.

# Exécution des Tests Unitaires & CI
Lancer les tests unitaires locaux
Les tests valident les règles métiers du traitement de flux sans nécessiter de connexion à Kafka :

```bash
pytest -v
```
Tester la CI GitHub Actions en local (avec act)
Si vous avez installé act, vous pouvez valider le pipeline de CI avant de pousser votre code sur GitHub :

```bash
act -W .github/workflows/tests.yml
```
# Règles Métier Traitées
Le composant processor.py évalue les règles suivantes sur les stations ouvertes :
```text
EMPTY_STATION : Déclenché si bikes_available == 0 (Rupture de stock).

LOW_STOCK : Déclenché si bikes_available <= 15 (Stock critique).

FULL_STATION : Déclenché si docks_available == 0 (Station saturée).
```
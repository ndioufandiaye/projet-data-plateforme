# Data Platform 🚀

## Stack
- Docker
- Spark
- MinIO
- Python
- Marimo/Notebook

## Sources de données 
 - Fichier CSV
 - Mysql
 - MongoDb

## sur la base mysql(dataplateforme) on a plusieurs tables mais ces deux tables qu'on a manipulées
tables
├── devis
├── client

## Base de données mongodb(gescom)
tables
├── employes

## pour le fichier csv on a recupéré les clients dépuis la base mysql qu'on a sauvegarder sur ce fichier pour pourvoir par la suite faire des jointure entre le fichier et la table devis de la base mysql
fichier csv
├── data/client.csv

## Setup

```bash
git clone ...
cd data-platform-projetISI

Assurez-vous que vous avez:

data-platform-projetISI/
├── docker-compose.yaml
├── requirements.txt
├── data/  ## dans lequel seront sauvegardé le fichier client.csv générés dépuis la table client de notre base dataplateforme
├── README.md 
├── Dockerfile
├── notebooks/
│   └── ingestion_mongodb.py
    └── gescom.employes.py
    └── silverTogold.py
    └── ingestion_dbsqlmysql.py
    ├── data/  ## dans lequel seront sauvegardé le fichier client.csv générés dépuis la table client de notre base dataplateforme


## création et activation env virtuel

python -m venv venv
venv\Scripts\activate     # Windows
 
 ## installation des dépendances 
pip install -r requirements.txt


## installation des dépendances avec pyproject.toml à la place de requirements.txt
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev


## Commande docker

# Démarrer les services
docker-compose up -d

# Arrêter les services
docker-compose down

# Voir les logs
docker-compose logs -f [service_name]

# Redémarrer un service
docker-compose restart [service_name]

# Reconstruire les images
docker-compose build --no-cache

# Tout nettoyer (ATTENTION: supprime les données)
docker-compose down -v

### pour lister les volumes docker
docker volume ls

### pour à la base mysql depuis le containeur
docker exec -it mysql mysql -u tp_user -p 

## pour lancer minio et marimo depuis le navigateur
MinIO
    Ouvrir http://localhost:9001
    Identifiants:
        user: minioadmin
        password: minioadmin123
    Vérifier que les buckets existent:
        bronze
        silver
        gold

Marimo Notebook: http://localhost:8080

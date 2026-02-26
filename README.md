# Data Platform 🚀

## Stack
- Docker
- Spark
- MinIO
- Python
- Jupyter / Marimo

## Setup

```bash
git clone ...
cd data-platform-projetISI

## création et activation env virtuel

python -m venv venv
venv\Scripts\activate     # Windows

pip install -r requirements.txt
docker-compose up -d


## installation des dépendances avec pyproject.toml à la place de requirements.txt
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

## ajout du driver PostgreSQL dans le conteneur Spark Marimo 
télécharger le driver depuis ce lien : https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

## Copier le jar dans ton conteneur Spark
docker cp C:\Users\DELL\Downloads\postgresql-42.6.0.jar sales_marimo_spark:/usr/local/lib/python3.11/site-packages/pyspark/jars/
## Se positionner dans le container marimo puis verifié si le jar est bien copié avec ces commandes
docker exec -it sales_marimo_spark bash 
find / -type d -name "jars" 2>/dev/null

## pour lister le volume docker
docker volume ls
## pour executé la base postegres depuis le containeur
docker exec -it sales_postgres psql -U postgres -d testspark

## pour lancer minio et marimo depuis le navigateur
MinIO console → http://localhost:9001

Marimo → http://localhost:8080

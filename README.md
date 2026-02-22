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

## pour lancer minio et marimo depuis le navigateur
MinIO console → http://localhost:9001

Marimo → http://localhost:8080

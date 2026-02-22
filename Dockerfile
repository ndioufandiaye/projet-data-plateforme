FROM eclipse-temurin:17-jdk

USER root

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Création du venv
RUN python3 -m venv /opt/venv

# Activer le venv dans PATH
ENV PATH="/opt/venv/bin:$PATH"

# Copier requirements
COPY requirements.txt .

# Installer dans le venv
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

USER 1000
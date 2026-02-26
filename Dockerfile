FROM python:3.11

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip install --no-cache-dir uv

# COPY pyproject.toml uv.lock* ./
# RUN uv sync --no-dev

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p /app/notebooks /app/data

# # Copier le driver PostgreSQL JDBC dans Spark
# COPY jars/postgresql-42.6.0.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/

# # Dockerfile
# RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.325.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.325/aws-java-sdk-bundle-1.12.325.jar

# Installer PostgreSQL JDBC Driver
COPY ./postgresql-42.6.0.jar /opt/spark/jars/

# Installer Hadoop AWS et AWS SDK pour S3A (MinIO)
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.325.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.325/aws-java-sdk-bundle-1.12.325.jar

# Définir PYSPARK_SUBMIT_ARGS pour inclure PostgreSQL
ENV PYSPARK_SUBMIT_ARGS="--packages org.postgresql:postgresql:42.7.3 pyspark-shell"

# Point d'entrée par défaut
# CMD ["/opt/bitnami/spark/bin/spark-shell"]

EXPOSE 8080

CMD ["uv", "run", "marimo", "edit", "notebooks/delta_lake_example.py", "--host", "0.0.0.0", "--port", "8080", "--no-token"]
#CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8080", "--no-browser", "--allow-root"]
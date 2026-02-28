import marimo

__generated_with = "0.20.2"
app = marimo.App(
    width="full",
    app_title="Data Plateforme - Ingestion MySQL/Csv",
)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data Plateforme avec PySpark, MySQL et MinIO

    ## Architecture

    ```
    MySQL (source de données)
          │
          ▼
    PySpark (moteur de traitement)
          │
          ▼
    MinIO / S3 (bronze - format Parquet)
    ```

    ---

    ## Configuration de l'environnement
    """)
    return


@app.cell
def _():
    import os
    from datetime import datetime

    # ── Connexion MySQL ──────────────────────────────────────────────────────────
    MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER     = os.getenv("MYSQL_USER", "tp_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "tp_password")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dataplateform")

    # JDBC URL pour Spark
    JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"
    JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

    # ── MinIO / S3 ───────────────────────────────────────────────────────────────
    MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    # Chemins S3
    MYSQ_PATH = "s3a://bronze/mysql"
    CSV_PATH = "s3a://bronze/csv/"

    TABLE_NAME = "devis"

    # ── Spark ────────────────────────────────────────────────────────────────────
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    print("✓ Configuration chargée")
    print(f"  MySQL  : {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    print(f"  MinIO  : {MINIO_ENDPOINT}")
    print(f"  Spark  : {SPARK_MASTER}")
    return (
        JDBC_DRIVER,
        JDBC_URL,
        MINIO_ACCESS_KEY,
        MINIO_ENDPOINT,
        MINIO_SECRET_KEY,
        MYSQL_DATABASE,
        MYSQL_HOST,
        MYSQL_PASSWORD,
        MYSQL_PORT,
        MYSQL_USER,
        MYSQ_PATH,
        SPARK_MASTER,
        TABLE_NAME,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Initialisation de la session Spark
    """)
    return


@app.cell
def _(MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, SPARK_MASTER):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F


    # Packages nécessaires (téléchargés automatiquement par Spark)
    packages = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "mysql:mysql-connector-java:8.0.33",
    ])

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("TP1-DataPipelines")
        .config("spark.jars.packages", packages)
        # ── Configuration S3A → MinIO ──────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Session Spark créée  →  version {spark.version}")
    return (spark,)


@app.cell
def _(JDBC_DRIVER, JDBC_URL, MYSQL_PASSWORD, MYSQL_USER, TABLE_NAME, spark):
    # ── Lecture complète de la table devis ─────────────────────────────────────
    query = f"(SELECT * FROM {TABLE_NAME}) AS table_sql"

    # TODO: Lire les données depuis les la base mysql (doc: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

    df_mysql = spark.read.format("jdbc") \
                    .option("url", JDBC_URL) \
                    .option("driver", JDBC_DRIVER) \
                    .option("user", MYSQL_USER) \
                    .option("password", MYSQL_PASSWORD) \
                    .option("dbtable", query) \
                    .load()

    print(f"✓ Données lues depuis MySQL")
    print(f"  Nombre de lignes  : {df_mysql.count()}")
    print(f"  Nombre de colonnes: {len(df_mysql.columns)}")
    print(f"\nSchéma :")
    df_mysql.printSchema()
    df_mysql.show(5)
    return (df_mysql,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Écriture données mysql dans MinIO (bronze/mysql)
    """)
    return


@app.cell
def _(MYSQ_PATH, df_mysql):

    df_mysql.write.format("parquet").mode("overwrite").save(MYSQ_PATH)

    print(f"✓ Load terminé !")
    print(f"  Destination : {MYSQ_PATH}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Lecture donnée source mysql depuis MinIO
    """)
    return


@app.cell
def _(MYSQ_PATH, spark):
    df_bronze_check = spark.read.parquet(MYSQ_PATH)

    print(f"✓ Vérification Bronze Layer")
    print(f"  Lignes lues depuis MinIO : {df_bronze_check.count()}")
    df_bronze_check.show(5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data Plateforme avec PySpark, fichier csv et MinIO

    ## Architecture

    ```
    Fichier csv (source de données)
          │
          ▼
    Python/spark (moteur de traitement)
          │
          ▼
    MinIO / S3 (Data Lake - format Parquet)
    ```

    ---

    ## Configuration de l'environnement
    """)
    return


@app.cell
def _(MYSQL_DATABASE, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER):
    import mysql.connector
    import csv
        # -------------------------------
        # 1️⃣ Connexion MySQL Docker
        # -------------------------------
    conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM client")
    donnees = cursor.fetchall()
    colonnes = [i[0] for i in cursor.description]

        # Écriture du CSV local
    local_csv = "/app/data/client.csv"
    with open(local_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(colonnes)
            writer.writerows(donnees)

    cursor.close()
    conn.close()
    return (local_csv,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Écriture du fichier CSV dans MinIO (bronze/csv)
    """)
    return


@app.cell
def _(MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, local_csv):
    # Configuration du client S3 (MinIO)
    import boto3
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    # Upload du fichier CSV vers MinIO
    bucket_name = "bronze"
    object_key = "csv/client.csv"
    s3_client.upload_file(local_csv, bucket_name, object_key)
    print(f"✓ Fichier CSV uploadé vers MinIO : s3://{bucket_name}/{object_key}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Lecture données source csv depuis MinIO
    """)
    return


@app.cell
def _(spark):
    # Lecture CSV depuis MinIO
    df = spark.read.csv(
        "s3a://bronze/csv/client.csv",
        header=True,
        inferSchema=True
    )

    df.show()
    return


if __name__ == "__main__":
    app.run()

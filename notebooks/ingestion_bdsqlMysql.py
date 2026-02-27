import marimo

__generated_with = "0.19.11"
app = marimo.App(width="full", app_title="TP1 - Data Pipelines")


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
    MinIO / S3 (Data Lake - format Parquet)
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
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "warehouse")

    # JDBC URL pour Spark
    JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"
    JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

    # ── MinIO / S3 ───────────────────────────────────────────────────────────────
    MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    # Chemins S3
    FULL_PATH = "s3a://bronze/full/orders"
    CSV_PATH = "s3a://bronze/csv/test"

    TABLE_NAME = "orders"

    # ── Spark ────────────────────────────────────────────────────────────────────
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    print("✓ Configuration chargée")
    print(f"  MySQL  : {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    print(f"  MinIO  : {MINIO_ENDPOINT}")
    print(f"  Spark  : {SPARK_MASTER}")
    return (
        FULL_PATH,
        CSV_PATH,
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
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

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
    # ── Lecture complète de la table orders ─────────────────────────────────────
    full_query = f"(SELECT * FROM {TABLE_NAME}) AS full_table"

    # TODO: Lire les données depuis les la base mysql (doc: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

    df = spark.read.format("jdbc") \
                    .option("url", JDBC_URL) \
                    .option("driver", JDBC_DRIVER) \
                    .option("user", MYSQL_USER) \
                    .option("password", MYSQL_PASSWORD) \
                    .option("dbtable", full_query) \
                    .load()

    print(f"✓ Données lues depuis MySQL")
    print(f"  Nombre de lignes  : {df.count()}")
    print(f"  Nombre de colonnes: {len(df.columns)}")
    print(f"\nSchéma :")
    df.printSchema()
    return df, full_query


@app.cell
def _(df):
    # Aperçu des données
    df.show(5, truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Étape 1.2 — Écriture dans MinIO (datalake)
    """)
    return


@app.cell
def _(FULL_PATH, df):

    df.write.format("parquet").mode("overwrite").save(FULL_PATH)

    print(f"✓ Load terminé !")
    print(f"  Destination : {FULL_PATH}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Lecture donnée source mysql depuis MinIO
    """)
    return


@app.cell
def _(FULL_PATH, spark):
    df_bronze_check = spark.read.parquet(FULL_PATH)

    print(f"✓ Vérification Bronze Layer")
    print(f"  Lignes lues depuis MinIO : {df_bronze_check.count()}")
    df_bronze_check.show(5)
    return (df_bronze_check,)

@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data Plateforme avec PySpark, fichier csv et MinIO

    ## Architecture

    ```
    Fichier csv (source de données)
          │
          ▼
    PySpark (moteur de traitement)
          │
          ▼
    MinIO / S3 (Data Lake - format Parquet)
    ```

    ---

    ## Configuration de l'environnement
    """)
    return

@app.cell()
def _(spark):
    # 🔹 On n’importe pas os à l’intérieur de la fonction si déjà importé
    # import os

    # 2️⃣ Chemin local du dossier data
    data_dir = "/app/data"  # chemin dans le container
    bucket_path = "s3a://bronze/csv/"

    # 3️⃣ Boucle sur tous les fichiers CSV
    for file in os.listdir(data_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(data_dir, file)
            print(f"Chargement de {file_path} → {bucket_path}{file}")
            
            df_csv = spark.read.option("header", "true").csv(file_path)
            df_csv.write.mode("overwrite").parquet(f"{bucket_path}{file.replace('.csv','/')}")
            
            print(f"{file} chargé avec succès ✅")

@app.cell
def _(mo):
    mo.md(r"""
    ## Lecture données source csv depuis MinIO
    """)
    return

@app.cell
def _(CSV_PATH, spark):
    df_bronze_csv = spark.read.parquet(CSV_PATH)

    print(f"✓ Vérification Bronze Layer")
    print(f"  Lignes lues depuis MinIO : {df_bronze_csv.count()}")
    df_bronze_csv.show(5)
    return (df_bronze_csv,)

if __name__ == "__main__":
    app.run()
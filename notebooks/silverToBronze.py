import marimo

__generated_with = "0.20.2"
app = marimo.App(
    width="full",
    app_title="Data Plateforme - Ingestion Silvet et Gold",
)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ```
    Bronze (source de données)
          │
          ▼
    PySpark (moteur de traitement)
          │
          ▼
    MinIO / S3 (silver/gold - format Parquet)
    ```
    """)
    return


@app.cell
def _():
    # ── MinIO / S3 ───────────────────────────────────────────────────────────────
    import os
    MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")


     # ── Spark ────────────────────────────────────────────────────────────────────
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    MYSQ_PATH = "s3a://bronze/mysql"

    print("✓ Configuration chargée")
    print(f"  MinIO  : {MINIO_ENDPOINT}")
    print(f"  Spark  : {SPARK_MASTER}")
    return (
        MINIO_ACCESS_KEY,
        MINIO_ENDPOINT,
        MINIO_SECRET_KEY,
        MYSQ_PATH,
        SPARK_MASTER,
    )


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
        .appName("silver-and-bronze")
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
def _(mo):
    mo.md(r"""
    ## Devis et clients depuis MinIO (bronze)
    """)
    return


@app.cell
def _(MYSQ_PATH, spark):
    # Lecture CSV clients depuis Bronze
    df_clients = spark.read.csv("s3a://bronze/csv/client.csv", header=True, inferSchema=True)

     # Lecture devis depuis Bronze (si tu as exporté depuis MySQL)
    df_devis = spark.read.parquet(MYSQ_PATH)

    print("✓ Données lues depuis Bronze")
    print(f"  Clients : {df_clients.count()} lignes")   
    print(f"  Devis   : {df_devis.count()} lignes")
    df_devis.show(5)
    return df_clients, df_devis


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Nettoyage des données (Silver)
    """)
    return


@app.cell
def _(df_clients, df_devis):

    from pyspark.sql.functions import trim, col

    df_clients_silver = df_clients.dropDuplicates() \
        .dropna() \
        .withColumn("email", trim(col("email")))

    # Sauvegarde Silver clients
    df_clients_silver.write.mode("overwrite").parquet("s3a://silver/csv/client")
    print("✅ Clients sauvegardés dans Silver")

    # Filtrer seulement les devis valides
    df_devis_silver = df_devis.filter(col("statut") == "acceptÃ©") \
        .dropDuplicates() \
        .dropna()

    # Sauvegarde Silver devis
    df_devis_silver.write.mode("overwrite").parquet("s3a://silver/mysql/devis")
    print("✅ Devis sauvegardés dans Silver")
    print(f"Nombre de devis sauvegardés : {df_devis_silver.count()}")
    return (df_devis_silver,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Agrégation + jointure (Gold)
    """)
    return


@app.cell
def _(df_devis_silver):
    from pyspark.sql.functions import count

    df_result = df_devis_silver.groupBy("id_client").agg(
        count("*").alias("nb_devis_valide")
    )

    df_result.show()
    return (df_result,)


@app.cell
def _(df_result):
    # Sauvegarde Gold
    df_result.write.mode("overwrite").parquet("s3a://gold/result")
    print("✅ Résulta sauvegardés dans le gold")
    print(f"Nombre de devis sauvegardés : {df_result.count()}")
    return


if __name__ == "__main__":
    app.run()

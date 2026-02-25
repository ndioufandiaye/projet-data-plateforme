import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def create_spark_session():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
    .appName("Ingestion Bronze") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    return (spark,)


@app.cell
def load_postgres_to_bronze(spark):
    # Lire les données de PostgreSQL
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5432/testspark")
        .option("dbtable", "actor")
        .option("user", "postgres")
        .option("password", "Ndiouf@1234")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Vérifier que le bucket Bronze existe avant d'écrire
    df.write \
      .mode("overwrite") \
      .parquet("s3a://bronze/actor")

    print("✓ Données chargées avec succès")
    print(f"Nombre d'enregistrements: {df.count()}")
    df.show()

    return


if __name__ == "__main__":
    app.run()

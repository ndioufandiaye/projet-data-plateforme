import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def _():
    from pyspark.sql import SparkSession

    # 1️⃣ Initialisation Spark avec config MinIO
    spark = (
        SparkSession.builder
        .appName("Ingestion Bronze")
        .master("local[*]")
        .config("spark.jars", "/app/jars/postgresql-42.6.0.jar")  # jar driver
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    # 2️⃣ Lecture PostgreSQL
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/testspark")
        .option("dbtable", "public.actor")
        .option("user", "postgres")
        .option("password", "Ndiouf@1234")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("✅ Données lues depuis PostgreSQL :")
    df.show(5)

    df.write.mode("overwrite").parquet("s3a://bronze/test")
    return


if __name__ == "__main__":
    app.run()

import marimo

app = marimo.App()

@app.cell
def _():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
    .appName("Tutoriel Spark") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    
    return spark

@app.cell
def _(spark):
    # Lire un fichier CSV avec en-tête
    df = spark.read.csv(
        "/app/data/salary_data.csv",
        header=True,
        inferSchema=True
    )

    print("✓ Données chargées avec succès")
    print(f"Nombre d'enregistrements: {df.count()}")
    df.show()
    return (df,)

if __name__ == "__main__":
    app.run()
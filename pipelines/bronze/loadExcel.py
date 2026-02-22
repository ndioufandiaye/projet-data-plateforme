from pyspark.sql import SparkSession
import pandas as pd

# Créer la SparkSession avec les packages Hadoop AWS
spark = SparkSession.builder \
    .appName("LoadExcelToBronze") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Définir le niveau de log à WARN pour moins de bruit
spark.sparkContext.setLogLevel("WARN")

# Exemple : lecture d'un Excel (pandas ou openpyxl) en DataFrame Spark
# spark_df = spark.read.format("...").load("...")

# Exemple : écrire dans S3 / MinIO
# spark_df.write.mode("overwrite").parquet("s3a://bronze/excelSource/")


# Lire Excel
df = pd.read_excel("data/fichierSources/Excel.xlsx")

# Convertir en Spark
spark_df = spark.createDataFrame(df)

# Écrire dans MinIO (Bronze)
spark_df.write.mode("overwrite").parquet("s3a://bronze/excelSource/")
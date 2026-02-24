import os
from datetime import datetime
from pyspark.sql import SparkSession

print("Base de données MongoDB → Spark DataFrame → Stockage Parquet dans Bronze (Medallion Architecture)")

# ====================== SPARK SESSION (Best Practices) ======================
spark = (
    SparkSession.builder
    .appName("ProjetDSIA_MongoDB_Gescom_Employes_Bronze")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")
    
    # === Configuration MinIO (S3 compatible) ===
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Optionnel : optimisation petite/moyenne collection
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

# ====================== CONNEXION MONGODB (credentials sécurisées) ======================
mongo_uri = "mongodb://negui:Nekarimo%401@host.docker.internal:27017/gescom.employes?authSource=admin"

spark.conf.set("spark.mongodb.read.connection.uri", mongo_uri)

# Lecture avec Spark Connector (format officiel MongoDB)
df = spark.read.format("mongodb").load()

print(f"{df.count():,} documents chargés depuis gescom.employes")
df.printSchema()           # Pour voir la structure (très utile pour la doc)
df.show(5, truncate=False) # Aperçu des 5 premières lignes

# ====================== ÉCRITURE DANS BRONZE (architecture Medallion) ======================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
bronze_path = f"s3a://bronze/raw_mongodb/employes/{timestamp}/"

df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(bronze_path)

print(f"INGESTION TERMINÉE !")
print(f"   Chemin Bronze : {bronze_path}")
print(f"   Format : Parquet (optimisé Spark + Snappy)")

# Bonus pro : stats rapides pour la doc
print(f"   → Nombre de partitions : {df.rdd.getNumPartitions()}")
print(f"   → Taille estimée : {df.rdd.countApprox(1000)/1000:.1f}k lignes")
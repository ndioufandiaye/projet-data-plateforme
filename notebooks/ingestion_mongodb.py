import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("=" * 80)
print("🚀 PROJET DSIA : INGESTION MONGODB (GESCOM) ➔ MINIO (BRONZE)")
print("=" * 80)

# =============================================================================
# 1. Configuration des URIs
# =============================================================================
# IMPORTANT : Dans Docker, on remplace 'localhost' par 'mongodb'
mongo_uri = "mongodb://negui:Nekarimo%401@sales_mongodb:27017/?authSource=admin"

# =============================================================================
# 2. Initialisation de la Session Spark (Spark 3.5 + Connecteurs)
# =============================================================================
spark = (
    SparkSession.builder
    .appName("Ingestion_Mongo_Gescom")
    # Chargement des JARs pour MongoDB et le protocole S3A (MinIO)
    .config("spark.jars.packages", 
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    
    # Configuration Hadoop pour MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # =============================================================================
    # 3. Lecture depuis MongoDB (Base: gescom, Collection: employes)
    # =============================================================================
    print("\n📥 Lecture des données depuis MongoDB (gescom.employes)...")
    
    df = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", mongo_uri)
        .option("database", "gescom")
        .option("collection", "employes")
        .load()
    )

    count = df.count()
    if count == 0:
        print("⚠️  Alerte : Aucune donnée trouvée dans la collection 'employes'.")
    else:
        print(f"✅ {count} documents récupérés avec succès !")
        print("\n--- Aperçu des données ---")
        df.show(5)
        df.printSchema()

    # =============================================================================
    # 4. Écriture vers MinIO (Couche Bronze - Architecture Medallion)
    # =============================================================================
    # Création d'un chemin avec timestamp pour l'historisation (Bonne pratique) [cite: 9]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_path = f"s3a://bronze/raw/mongodb/employes/{timestamp}/"

    print(f"\n📤 Écriture vers la zone BRONZE (MinIO) : {bronze_path}")
    
    # On sauvegarde au format Parquet (standard pour le Big Data) [cite: 14]
    df.write.mode("overwrite").parquet(bronze_path)

    print("\n" + "=" * 80)
    print("✨ OPÉRATION TERMINÉE : DONNÉES DISPONIBLES DANS MINIO")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERREUR CRITIQUE : {e}")

finally:
    # Arrêt de la session pour libérer les ressources
    spark.stop()
import marimo


__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def intro():
    import marimo as mo
    from pyspark.sql import SparkSession
    from datetime import datetime # Défini ici une seule fois pour tout le monde
    import json
    from pymongo import MongoClient
    from bson import ObjectId

    return MongoClient, ObjectId, SparkSession, datetime, json


@app.cell
def configuration(MongoClient):
    # URL de connexion utilisant le nom du service Docker
    client = MongoClient("mongodb://negui:Nekarimo%401@sales_mongodb:27017/gescom?authSource=admin")
    db = client.gescom
    return (db,)


@app.cell
def _(ObjectId, db, json):
    try:
        with open('gescom.employes.json', 'r') as f:
            donnees = json.load(f)

        # 2. NETTOYAGE : Conversion des $oid en vrais ObjectId MongoDB
        for doc in donnees:
            if '_id' in doc and isinstance(doc['_id'], dict) and '$oid' in doc['_id']:
                doc['_id'] = ObjectId(doc['_id']['$oid'])

        # 3. Insertion
        db.employes.drop()
        db.employes.insert_many(donnees)

        print(f"Réussi ! {db.employes.count_documents({})} employés importés avec succès.")
    except Exception as e:
        print(f"❌ Erreur : {e}")
    return


@app.cell
def _(spark):
    # Lecture via Spark
    # Utilise cette syntaxe précise qui force Spark à ignorer "localhost"
    df = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", "mongodb://negui:Nekarimo%401@sales_mongodb:27017/?authSource=admin")
        .option("database", "gescom")
        .option("collection", "employes")
        .load()
    )

    print(f"Succès ! Spark a trouvé {df.count()} documents.")
    df.show(5)
    return (df,)


@app.cell
def start_spark(SparkSession):
    spark = (
        SparkSession.builder
        .appName("Ingestion_Mongo_Final")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        # On utilise les versions spécifiques pour Spark 3.5
        .config("spark.jars.packages", 
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return (spark,)


@app.cell
def save_to_minio(datetime, df):
    #Envoyer vers MinIO
    # Cellule de sauvegarde vers MinIO
    if df is not None and df.count() > 0:
        # Utilise directement datetime
        horodatage = datetime.now().strftime("%Y%m%d_%H%M")
        chemin_final = f"s3a://bronze/mongodb/employes/{horodatage}/"
    
        print(f"Envoi de {df.count()} lignes vers MinIO...")
        df.write.mode("overwrite").parquet(chemin_final)
    
        print(f"✨ Ingestion terminée : {chemin_final}")
    return


@app.cell
def _(df):
    # Cellule de sauvegarde (simplifiée)
    chemin_fixe = "s3a://bronze/mongodb/employes/donnees_finales/"

    if df is not None and df.count() > 0:
        print(f"📤 Envoi vers MinIO...")
        # On utilise 'overwrite' pour écraser les anciens tests
        df.write.mode("overwrite").parquet(chemin_fixe)
        print(f"✨ Ingestion terminée dans : {chemin_fixe}")
    return


@app.cell
def _(spark):
    # Cellule de lecture
    df_minio = spark.read.parquet("s3a://bronze/mongodb/employes/donnees_finales/")
    print(f"✅ Lecture réussie ! Total : {df_minio.count()} employés.")
    df_minio.show(5)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

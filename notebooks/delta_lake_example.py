from pyspark.sql import SparkSession

# Création de la session Spark, en se connectant au master Spark via le réseau Docker
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("TestDeltaLake") \
    .getOrCreate()

# Création d'un DataFrame simple
df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])

# Affichage du DataFrame
df.show()

# Arrêt propre de la session Spark
spark.stop()
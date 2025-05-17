# streamingfile.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Schéma enrichi avec toutes les colonnes nécessaires
schema = StructType() \
    .add("Date", StringType()) \
    .add("Div", StringType()) \
    .add("HomeTeam", StringType()) \
    .add("AwayTeam", StringType()) \
    .add("FTR", StringType()) \
    .add("B365H", StringType()) \
    .add("B365D", StringType()) \
    .add("B365A", StringType()) \
    .add("PSH", StringType()) \
    .add("PSD", StringType()) \
    .add("PSA", StringType()) \
    .add("BbAvH", StringType()) \
    .add("BbAvD", StringType()) \
    .add("BbAvA", StringType()) \
    .add("BbMxH", StringType()) \
    .add("BbMxD", StringType()) \
    .add("BbMxA", StringType()) \
    .add("FTHG", StringType()) \
    .add("FTAG", StringType()) \
    .add("HTHG", StringType()) \
    .add("HTAG", StringType()) \
    .add("HS", StringType()) \
    .add("AS", StringType()) \
    .add("HST", StringType()) \
    .add("AST", StringType()) \
    .add("HC", StringType()) \
    .add("AC", StringType()) \
    .add("HF", StringType()) \
    .add("AF", StringType()) \
    .add("HY", StringType()) \
    .add("AY", StringType()) \
    .add("HR", StringType()) \
    .add("AR", StringType()) \
    .add("HBP", StringType()) \
    .add("ABP", StringType()) \
    .add("Referee", StringType()) \
    .add("Season", StringType()) \
    .add("League", StringType())

# Définir les chemins locaux
root = "/home/jovyan/work/spark"

# URI MongoDB vers la nouvelle base et collection
mongo_uri = "mongodb://admin:admin@mongodb:27017/football_matches?authSource=admin"
#mongo_uri = "mongodb://mongo:27017/football_stream_db.matches_streamed"

# Créer la session Spark avec Kafka et MongoDB
spark = (
    SparkSession.builder
      .appName("FootballKafkaToMongo")
      .config("spark.jars.packages", \
              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
              "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
      .config("spark.mongodb.write.connection.uri", mongo_uri)
      .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lecture du flux Kafka (utiliser le même nom de topic que le producer)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "football-matches") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Transformation JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Sink console pour debug (30s)
console_query = (
    df_json.writeStream
        .format("console")
        .option("truncate", "false")
        .start()
)


# ─── 6) Stream to Parquet ──────────────────────────────────────────────
parquet_query = (
    df_json.writeStream
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", f"{root}/spark_checkpoints/parquet/matches")
        .option("path", f"{root}/output_parquet/matches")
        .start()
)

# ─── 7) Stream to MongoDB ──────────────────────────────────────────────
mongo_query = (
    df_json.writeStream
        .outputMode("append")
        .format("mongodb")
        .option("uri", "mongodb://admin:admin@mongodb:27017/football_matches?authSource=admin")
        .option("database", "football_matches")
        .option("collection", "matches")
        .option("checkpointLocation", f"{root}/spark_checkpoints/mongo/matches")
        .start()
)

# Attendre la fin des streams
console_query.awaitTermination(30)
parquet_query.awaitTermination()
mongo_query.awaitTermination()

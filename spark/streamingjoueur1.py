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

# Colonnes à conserver (HTR retiré car absent)
columns_to_keep = [
    'Div', 'Date', 'HomeTeam', 'AwayTeam',
    'FTHG', 'FTAG', 'FTR',
    'HTHG', 'HTAG',
    'HS', 'AS', 'HST', 'AST',
    'HC', 'AC',
    'HF', 'AF',
    'HY', 'AY', 'HR', 'AR',
    'GD', 'AGL5', 'HGL5', 'HTP', 'ATP',
    'ATAFL5', 'HTHFL5', 'ADIL5', 'HDIL5', 'ATFL5', 'HTFL5'
]

# URI MongoDB vers la nouvelle base et collection
mongo_uri = "mongodb://admin:admin@mongodb:27017/football11_matches?authSource=admin"
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
    .option("subscribe", "football11-matches") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Transformation JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")
# Convertir les colonnes numériques en entiers
numeric_columns = ['FTHG', 'FTAG', 'HTHG', 'HTAG', 'HS', 'AS', 'HST', 'AST', 'HC', 'AC', 'HF', 'AF', 'HY', 'AY', 'HR', 'AR']
for c in numeric_columns:
    df_json = df_json.withColumn(c, col(c).cast(IntegerType()))


# Filtrer les saisons >= 2008-2009
df_json = df_json.filter(col("Season") >= "0809")


# Calculer GD (Goal Difference: FTHG - FTAG)
df_json = df_json.withColumn("GD", col("FTHG") - col("FTAG"))

# Définir les fenêtres pour les calculs sur les 5 derniers matchs
home_window = Window.partitionBy("HomeTeam", "Season").orderBy("Date").rowsBetween(-5, -1)
away_window = Window.partitionBy("AwayTeam", "Season").orderBy("Date").rowsBetween(-5, -1)
home_form_window = Window.partitionBy("HomeTeam", "Season").orderBy("Date").rowsBetween(-5, -1)
away_form_window = Window.partitionBy("AwayTeam", "Season").orderBy("Date").rowsBetween(-5, -1)
season_window = Window.partitionBy("Season").orderBy("Date").rowsBetween(Window.unboundedPreceding, -1)

# Calculer les points pour chaque match
df_json = df_json.withColumn(
    "HomePoints",
    when(col("FTR") == "H", 3).when(col("FTR") == "D", 1).otherwise(0)
).withColumn(
    "AwayPoints",
    when(col("FTR") == "A", 3).when(col("FTR") == "D", 1).otherwise(0)
)

# Calculer HTP (Home Team Points) et ATP (Away Team Points)
df_json = df_json.withColumn(
    "HTP",
    sum(when(col("HomeTeam") == df_json["HomeTeam"], col("HomePoints")).otherwise(0)).over(season_window) +
    sum(when(col("AwayTeam") == df_json["HomeTeam"], col("AwayPoints")).otherwise(0)).over(season_window)
).withColumn(
    "ATP",
    sum(when(col("HomeTeam") == df_json["AwayTeam"], col("HomePoints")).otherwise(0)).over(season_window) +
    sum(when(col("AwayTeam") == df_json["AwayTeam"], col("AwayPoints")).otherwise(0)).over(season_window)
)

# Calculer AGL5 (Away Team Goals Last 5) et HGL5 (Home Team Goals Last 5)
df_json = df_json.withColumn(
    "AGL5",
    sum(when(col("AwayTeam") == df_json["AwayTeam"], col("FTAG")).otherwise(0)).over(away_window)
).withColumn(
    "HGL5",
    sum(when(col("HomeTeam") == df_json["HomeTeam"], col("FTHG")).otherwise(0)).over(home_window)
)

# Calculer ATFL5 (Away Team Form Last 5) et HTFL5 (Home Team Form Last 5)
df_json = df_json.withColumn(
    "ATFL5",
    sum(when(col("AwayTeam") == df_json["AwayTeam"], col("AwayPoints")).otherwise(0)).over(away_window)
).withColumn(
    "HTFL5",
    sum(when(col("HomeTeam") == df_json["HomeTeam"], col("HomePoints")).otherwise(0)).over(home_window)
)

# Calculer ATAFL5 (Away Team Away Form Last 5) et HTHFL5 (Home Team Home Form Last 5)
df_json = df_json.withColumn(
    "ATAFL5",
    sum(when(col("AwayTeam") == df_json["AwayTeam"], col("AwayPoints")).otherwise(0)).over(away_form_window)
).withColumn(
    "HTHFL5",
    sum(when(col("HomeTeam") == df_json["HomeTeam"], col("HomePoints")).otherwise(0)).over(home_form_window)
)

# Calculer ADIL5 (Away Team Discipline Index Last 5) et HDIL5 (Home Team Discipline Index Last 5)
df_json = df_json.withColumn(
    "ADIL5",
    sum(when(col("AwayTeam") == df_json["AwayTeam"], (col("AF") * 2) + (col("AY") * 10) + (col("AR") * 25)).otherwise(0)).over(away_window)
).withColumn(
    "HDIL5",
    sum(when(col("HomeTeam") == df_json["HomeTeam"], (col("HF") * 2) + (col("HY") * 10) + (col("HR") * 25)).otherwise(0)).over(home_window)
)


# Sélectionner uniquement les colonnes spécifiées, ignorer si HTR est absente
available_columns = [c for c in columns_to_keep if c in df_json.columns]
df_json = df_json.select([col(c) for c in available_columns])


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
        .option("checkpointLocation", f"{root}/spark_checkpoints/parquet/matches11")
        .option("path", f"{root}/output_parquet/matches11")
        .start()
)

# ─── 7) Stream to MongoDB ──────────────────────────────────────────────
mongo_query = (
    df_json.writeStream
        .outputMode("append")
        .format("mongodb")
        .option("uri", "mongodb://admin:admin@mongodb:27017/football11_matches?authSource=admin")
        .option("database", "football11_matches")
        .option("collection", "matches")
        .option("checkpointLocation", f"{root}/spark_checkpoints/mongo/matches11")
        .start()
)

# Attendre la fin des streams
console_query.awaitTermination(30)
parquet_query.awaitTermination()
mongo_query.awaitTermination()

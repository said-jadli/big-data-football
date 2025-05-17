# streamingfile.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_date, when, coalesce, lit, sum as spark_sum
)
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.window import Window

# Schéma des données
schema = StructType() \
    .add("Date", StringType()) \
    .add("Div", StringType()) \
    .add("HomeTeam", StringType()) \
    .add("AwayTeam", StringType()) \
    .add("FTR", StringType()) \
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
    .add("Season", StringType())

# Chemins et URI
root = "/home/jovyan/work/spark"
mongo_uri = "mongodb://admin:admin@mongodb:27017/football11_matches?authSource=admin"

# Créer SparkSession avec politique de parsing legacy pour dates
spark = (
    SparkSession.builder
        .appName("FootballKafkaToMongo")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Fonction de traitement par micro-batch
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # Parse JSON et conversion types
    df = batch_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    # Cast numériques
    numeric_cols = ['FTHG','FTAG','HTHG','HTAG','HS','AS','HST','AST','HC','AC','HF','AF','HY','AY','HR','AR']
    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast(IntegerType()))
    # Parse Date: essayer dd/MM/yyyy puis yy/MM/dd
    df = df.withColumn(
        "Date", 
        coalesce(
            to_date(col("Date"), "dd/MM/yyyy"), 
            to_date(col("Date"), "yy/MM/dd")
        )
    )
    # Filtrer saisons
    df = df.filter(col("Season").cast(IntegerType()) >= 2008)

    # Définitions fenêtres
    season_w = Window.partitionBy("Season").orderBy("Date").rowsBetween(Window.unboundedPreceding, -1)
    home5_w = Window.partitionBy("HomeTeam","Season").orderBy("Date").rowsBetween(-5, -1)
    away5_w = Window.partitionBy("AwayTeam","Season").orderBy("Date").rowsBetween(-5, -1)

    # Feature engineering avec coalesce pour valeurs vides -> 0
    df_feat = df \
        .withColumn("GD", col("FTHG") - col("FTAG")) \
        .withColumn("HomePoints", when(col("FTR")=="H",3).when(col("FTR")=="D",1).otherwise(0)) \
        .withColumn("AwayPoints", when(col("FTR")=="A",3).when(col("FTR")=="D",1).otherwise(0)) \
        .withColumn("HTP", coalesce(spark_sum("HomePoints").over(season_w), lit(0)) + coalesce(spark_sum("AwayPoints").over(season_w), lit(0))) \
        .withColumn("ATP", coalesce(spark_sum("AwayPoints").over(season_w), lit(0)) + coalesce(spark_sum("HomePoints").over(season_w), lit(0))) \
        .withColumn("HGL5", coalesce(spark_sum("FTHG").over(home5_w), lit(0))) \
        .withColumn("AGL5", coalesce(spark_sum("FTAG").over(away5_w), lit(0))) \
        .withColumn("HTFL5", coalesce(spark_sum("HomePoints").over(home5_w), lit(0))) \
        .withColumn("ATFL5", coalesce(spark_sum("AwayPoints").over(away5_w), lit(0))) \
        .withColumn("HDIL5", coalesce(spark_sum(col("HF")*2 + col("HY")*10 + col("HR")*25).over(home5_w), lit(0))) \
        .withColumn("ADIL5", coalesce(spark_sum(col("AF")*2 + col("AY")*10 + col("AR")*25).over(away5_w), lit(0)))

    # Écriture Parquet & MongoDB
    (df_feat.write.format("parquet").mode("append")
         .option("path", f"{root}/output_parquet/matches11")
         .option("checkpointLocation", f"{root}/spark_checkpoints/parquet/matches11")
         .save())
    (df_feat.write.format("mongodb").mode("append")
         .option("uri", mongo_uri)
         .option("database", "football11_matches")
         .option("collection", "matches")
         .option("checkpointLocation", f"{root}/spark_checkpoints/mongo/matches11")
         .save())

# Démarrage du stream
(raw_stream := spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers", "kafka:9092")
     .option("subscribe", "football11-matches")
     .option("startingOffsets", "earliest")
     .option("failOnDataLoss", "false")
     .load())

query = raw_stream.writeStream.foreachBatch(process_batch)
query = query.option("checkpointLocation", f"{root}/spark_checkpoints/foreachBatch").start()
query.awaitTermination()

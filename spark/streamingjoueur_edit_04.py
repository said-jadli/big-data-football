from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, sum as _sum, when, to_date,
    coalesce, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType

# ─── 1) Schéma JSON ─────────────────────────────────────────────────────
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
    .add("Season", StringType()) \
    .add("League", StringType())

# ─── 2) Configs ──────────────────────────────────────────────────────────
root = "/home/jovyan/work/spark"
mongo_uri = "mongodb://admin:admin@mongodb:27017/football_matches?authSource=admin"

# ─── 3) Spark Session ────────────────────────────────────────────────────
spark = (
    SparkSession.builder
      .appName("FootballKafkaToMongo")
      .config("spark.jars.packages",
              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
              "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
      .config("spark.mongodb.write.connection.uri", mongo_uri)
      .getOrCreate()
)
# rétro compat dates
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# dossier temporaire
spark.conf.set("spark.local.dir", "/tmp_spark")
spark.sparkContext.setLogLevel("WARN")

# ─── 4) Lecture Kafka ────────────────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "football-matches") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# ─── 5) foreachBatch pour enrichir et sauvegarder ────────────────────────
def process_batch(batch_df, batch_id):
    # 1) types & date
    df = batch_df \
        .withColumn("FTHG", col("FTHG").cast("int")) \
        .withColumn("FTAG", col("FTAG").cast("int")) \
        .withColumn("HY", col("HY").cast("int")) \
        .withColumn("HR", col("HR").cast("int")) \
        .withColumn("AY", col("AY").cast("int")) \
        .withColumn("AR", col("AR").cast("int")) \
        .withColumn("MatchDate", to_date(col("Date"), "dd/MM/yyyy"))

    # 2) points par match
    df = df.withColumn("HomeResultScore",
                       when(col("FTR") == "H", 3)
                       .when(col("FTR") == "D", 1)
                       .otherwise(0)) \
           .withColumn("AwayResultScore",
                       when(col("FTR") == "A", 3)
                       .when(col("FTR") == "D", 1)
                       .otherwise(0))

    # 3) discipline index par match
    df = df.withColumn("HDI", col("HY") * lit(5) + col("HR") * lit(25)) \
           .withColumn("ADI", col("AY") * lit(5) + col("AR") * lit(25))

    # 4) fenêtres sliding pour les 5 derniers matchs (home / away)
    home_win = Window.partitionBy("Season","HomeTeam") \
                     .orderBy("MatchDate").rowsBetween(-5, -1)
    away_win = Window.partitionBy("Season","AwayTeam") \
                     .orderBy("MatchDate").rowsBetween(-5, -1)

    df = df.withColumn("HGL5", _sum("FTHG").over(home_win)) \
           .withColumn("AGL5", _sum("FTAG").over(away_win)) \
           .withColumn("HTHF5", _sum("HomeResultScore").over(home_win)) \
           .withColumn("ATAF5", _sum("AwayResultScore").over(away_win)) \
           .withColumn("HDIL5", _sum("HDI").over(home_win)) \
           .withColumn("ADIL5", _sum("ADI").over(away_win))

    # 5) form last 5 games regardless of venue (HTFL5 / ATFL5)
    # on crée un DF unifié Team+score et on réassocie
    df_team = df.select(
        col("Season"),
        col("MatchDate"),
        col("HomeTeam").alias("Team"),
        col("HomeResultScore").alias("PTS")
    ).union(
        df.select(
            col("Season"),
            col("MatchDate"),
            col("AwayTeam").alias("Team"),
            col("AwayResultScore").alias("PTS")
        )
    )
    team_win = Window.partitionBy("Season","Team") \
                     .orderBy("MatchDate").rowsBetween(-5, -1)
    df_team = df_team.withColumn("LAST5_PTS", _sum("PTS").over(team_win))

    # on récupère HTFL5 et ATFL5 par jointure
    df = df.join(
        df_team.select("Season","MatchDate", col("Team").alias("HomeTeam"), col("LAST5_PTS").alias("HTFL5")),
        on=["Season","MatchDate","HomeTeam"], how="left"
    ).join(
        df_team.select("Season","MatchDate", col("Team").alias("AwayTeam"), col("LAST5_PTS").alias("ATFL5")),
        on=["Season","MatchDate","AwayTeam"], how="left"
    )

    # 6) season cumul points (HTSP / ATSP)
    # points accumulés à domicile + à l’extérieur
    home_pts = df.groupBy("Season","HomeTeam") \
                 .agg(_sum("HomeResultScore").alias("HT_PTS_HOME"),
                      _sum("AwayResultScore").alias("HT_PTS_AWAY")
                 )
    away_pts = df.groupBy("Season","AwayTeam") \
                 .agg(_sum("AwayResultScore").alias("AT_PTS_AWAY"),
                      _sum("HomeResultScore").alias("AT_PTS_HOME")
                 )
    # on crée la colonne totale
    df = df.join(home_pts, on=["Season","HomeTeam"], how="left") \
           .withColumn("HTSP", col("HT_PTS_HOME") + col("HT_PTS_AWAY")) \
           .join(away_pts, on=["Season","AwayTeam"], how="left") \
           .withColumn("ATSP", col("AT_PTS_HOME") + col("AT_PTS_AWAY"))

    # 7) conserver le reste de ton enrichissement (HTASG, ATASG, SGD etc.)
    ht_agg = df.groupBy("Season","HomeTeam") \
               .agg(_sum("FTHG").alias("HTASG"))
    at_agg = df.groupBy("Season","AwayTeam") \
               .agg(_sum("FTAG").alias("ATASG"))
    df = df.join(ht_agg, ["Season","HomeTeam"], "left") \
           .join(at_agg, ["Season","AwayTeam"], "left") \
           .withColumn("SGD", col("HTASG") - col("ATASG"))

    # --- écriture en base & Parquet (idem à avant) ---
    df.repartition(1).write \
      .mode("append") \
      .parquet(f"{root}/output_parquet/matches")

    df.write \
      .format("mongodb") \
      .mode("append") \
      .option("uri", mongo_uri) \
      .option("database", "football_matches") \
      .option("collection", "matches_enriched") \
      .save()

# ─── 6) démarrage du stream ─────────────────────────────────────────────
query = df_json.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", f"{root}/spark_checkpoints/foreach") \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler
import random
import time

kafka_topic_name = "src-app-users-json"
kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession \
        .builder \
        .appName("Structured Streaming ") \
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Construct a streaming DataFrame that reads from topic
flower_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

flower_df1 = flower_df.selectExpr("CAST(value AS STRING)", "timestamp")

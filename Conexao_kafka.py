import findspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from kafka import KafkaProducer

#findspark.add_packages("org.apache.spark:spark-sql-kafka-0-10_2.12")
import os
# main app
if __name__ == '__main__':

    # init spark session
    spark = SparkSession \
            .builder \
            .appName("pr-movies-analysis-batch") \
            .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching","false") \
            .getOrCreate()

    # set log level to info
    # [INFO] or [WARN] for more detailed logging info
    spark.sparkContext.setLogLevel("INFO")

    # Subscribe to 1 topic
    kafka_topic_name = "src-app-users-json"
    kafka_bootstrap_servers = 'localhost:9092'
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

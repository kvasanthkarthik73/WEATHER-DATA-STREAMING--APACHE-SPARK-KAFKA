from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

# Create a SparkSession
spark = SparkSession.builder \
    .appName("weather") \
    .getOrCreate()
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,mysql:mysql-connector-java:8.0.26") \
    

# Define the schema for the weather data
schema = StructType([
    StructField("FormattedDate", StringType(), True),
    StructField("Summary", StringType(), True),
    StructField("PrecipType", StringType(), True),
    StructField("Temperature (C)", DoubleType(), True),
    StructField("ApparentTemperature_C", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("WindSpeed_kmh", DoubleType(), True),
    StructField("WindBearing_degrees", DoubleType(), True),
    StructField("Visibility_km", DoubleType(), True),
    StructField("LoudCover", DoubleType(), True),
    StructField("Pressure_millibars", DoubleType(), True),
    StructField("DailySummary", StringType(), True)
])

# Define the Kafka parameters
brokers = 'localhost:9092'
topics = ['weather']

# Read from the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse the JSON data and create a DataFrame
df = df \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("Temperature (C)", col("Temperature (C)").cast("double")) \
    .withColumn("ApparentTemperature_C", col("ApparentTemperature_C").cast("double")) \
    .withColumn("Humidity", col("Humidity").cast("double")) \
    .withColumn("WindSpeed_kmh", col("WindSpeed_kmh").cast("double")) \
    .withColumn("WindBearing_degrees", col("WindBearing_degrees").cast("double")) \
    .withColumn("Visibility_km", col("Visibility_km").cast("double")) \
    .withColumn("LoudCover", col("LoudCover").cast("double")) \
    .withColumn("Pressure_millibars", col("Pressure_millibars").cast("double"))

# Aggregate the data by weather attribute and count the occurrences
counts_df = df.groupby("Temperature (C)").count()

# Write the result DataFrame to MySQL
mysql_url = "jdbc:mysql://localhost:3306/projdatabase"
mysql_properties = {
    "user": "root",
    "password": "rootpass",
    "driver": "com.mysql.cj.jdbc.Driver"
}

query = counts_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: df.write.jdbc(url=mysql_url, table="weather2", mode="append", properties=mysql_properties)) \
    .start()
    
   
query.awaitTermination()

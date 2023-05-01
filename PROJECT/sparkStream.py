from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .getOrCreate()

start_time = datetime.now()

# Define the schema for the weather data
schema = StructType([
    StructField("FormattedDate", StringType(), True),
    StructField("Summary", StringType(), True),
    StructField("PrecipType", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("ApparentTemperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("WindSpeed", DoubleType(), True),
    StructField("WindBearing", DoubleType(), True),
    StructField("Visibility", DoubleType(), True),
    StructField("LoudCover", DoubleType(), True),
    StructField("Pressure", DoubleType(), True),
    StructField("DailySummary", StringType(), True)
])

# Read in the weather data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data and select the fields we're interested in
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")).select("data.*")

# Do any additional transformations on the data here

# Write the stream to a MySQL database
mysql_url = "jdbc:mysql://localhost:3306/dbtproj"
mysql_properties = {
    "user": "root",
    "password": "rootpass",
    "driver": "com.mysql.jdbc.Driver"
}

query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: batchDF.write.jdbc(mysql_url, "weather_data", mode="append", properties=mysql_properties)) \
    .start()

query.awaitTermination()

end_time = datetime.now()
duration = end_time - start_time

print(f"Processing completed in {duration.total_seconds()} seconds.")

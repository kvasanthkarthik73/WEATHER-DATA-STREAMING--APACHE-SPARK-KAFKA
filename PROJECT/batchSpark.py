from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime


spark = SparkSession.builder \
    .appName("BatchQuery") \
    .getOrCreate()

# Define the schema for the weather data
schema = StructType([
    StructField("formatted_date", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("precip_type", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("apparent_temperature_c", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed_km_h", DoubleType(), True),
    StructField("wind_bearing_degrees", DoubleType(), True),
    StructField("visibility_km", DoubleType(), True),
    StructField("loud_cover", DoubleType(), True),
    StructField("pressure_millibars", DoubleType(), True),
    StructField("daily_summary", StringType(), True)
])

# read the weather data from the CSV file
df = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("schema", schema) \
    .load("weather_data.csv")

# do some calculation on the temperature_c column
df = df.select("formatted_date", "temperature_c", (col("temperature_c") * 1.8 + 32).alias("temperature_f"))

# write the result to a new table in the MySQL database
mysql_url = "jdbc:mysql://localhost:3306/dbtproj"
mysql_properties = {
    "user": "root",
    "password": "rootpass",
    "driver": "com.mysql.jdbc.Driver"
}

start_time = datetime.now()

df.write.jdbc(mysql_url, "batch_weather_data", mode="overwrite", properties=mysql_properties)

end_time = datetime.now()
duration = end_time - start_time

print(f"Processing completed in {duration.total_seconds()} seconds.")

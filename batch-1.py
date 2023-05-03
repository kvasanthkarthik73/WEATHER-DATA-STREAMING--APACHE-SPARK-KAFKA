from pyspark.sql import SparkSession 
from pyspark.sql.functions import col 
# create a SparkSession 
spark = SparkSession.builder.appName("weather_batch").getOrCreate() 
# read data from MySQL database 
jdbc_url = "jdbc:mysql://localhost/projdatabase" 
connection_properties = { "user": "root", "password": "rootpass", "driver": "com.mysql.jdbc.Driver" } 
df = spark.read.jdbc(url=jdbc_url, table="weather2", properties=connection_properties) 
# process data 
df_with_temp_c = df.withColumn("Temperature (C)", col("Temperature (C)") * 5) 
# write result to MySQL database 
df_with_temp_c.write.jdbc(url=jdbc_url, table="weather3", mode="overwrite", properties=connection_properties) 
# stop SparkSession 
spark.stop()

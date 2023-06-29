import pandas as pd
import logging
import argparse
from pyspark.sql import SparkSession

# Ottieni il logger del modulo kafka
logger = logging.getLogger("ANALISI")
logger.setLevel(logging.DEBUG)  # Imposta il livello di logging desiderato

# Configura l'handler di logging per il logger di kafka
handler = logging.StreamHandler()  # Puoi modificare questo per inoltrare i log a un file
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
args = parser.parse_args()
input_filepath = args.input_path

spark = SparkSession.builder \
    .appName('ANALYSIS') \
    .config('spark.cassandra.connection.host', 'cassandra') \
    .config('spark.cassandra.connection.port', '9042') \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="preprocessed_row_dataset", keyspace="my_batch") \
    .load()

df.printSchema()

df.createOrReplaceTempView("df")

# Analisi consumi e generazione [kw]

kw_total_df = spark.sql("""
    SELECT 
        SUM(use_kw) AS use_kw_total,
        SUM(gen_kw) AS gen_kw_total,
        AVG(use_kw) AS use_kw_avg,
        AVG(gen_kw) AS gen_kw_avg,
        MAX(use_kw) AS use_kw_max,
        MAX(gen_kw) AS gen_kw_max,
        MIN(use_kw) AS use_kw_min,
        MIN(gen_kw) AS gen_kw_min
    FROM df
""")

kw_month_df = spark.sql("""
    SELECT MONTH(time) AS month,
            SUM(use_kw) AS use_kw_total,
            SUM(gen_kw) AS gen_kw_total,
            AVG(use_kw) AS use_kw_avg,
            AVG(gen_kw) AS gen_kw_avg,
            MAX(use_kw) AS use_kw_max,
            MAX(gen_kw) AS gen_kw_max,
            MIN(use_kw) AS use_kw_min,
            MIN(gen_kw) AS gen_kw_min
    FROM df
    GROUP BY MONTH(time)
""")

kw_dayofweek_df = spark.sql("""
    SELECT DAYOFWEEK(time) AS day_of_week,
            SUM(use_kw) AS use_kw_total,
            SUM(gen_kw) AS gen_kw_total,
            AVG(use_kw) AS use_kw_avg,
            AVG(gen_kw) AS gen_kw_avg,
            MAX(use_kw) AS use_kw_max,
            MAX(gen_kw) AS gen_kw_max,
            MIN(use_kw) AS use_kw_min,
            MIN(gen_kw) AS gen_kw_min
    FROM df
    GROUP BY DAYOFWEEK(time)
""")


kw_day_df = spark.sql("""
    SELECT DAYOFYEAR(time) AS day,
            SUM(use_kw) AS use_kw_total,
            SUM(gen_kw) AS gen_kw_total,
            AVG(use_kw) AS use_kw_avg,
            AVG(gen_kw) AS gen_kw_avg,
            MAX(use_kw) AS use_kw_max,
            MAX(gen_kw) AS gen_kw_max,
            MIN(use_kw) AS use_kw_min,
            MIN(gen_kw) AS gen_kw_min
    FROM df
    GROUP BY DAYOFYEAR(time)
""")

# Analisi tempo atmosferico 
weather_total_df = spark.sql("""
    SELECT AVG(temperature) AS avgTemp,
           MIN(temperature) AS minTemp,
           MAX(temperature) AS maxTemp,
           AVG(humidity) AS avgHum,
           MIN(humidity) AS minHum,
           MAX(humidity) AS maxHum,
           AVG(visibility) AS avgVis,
           MIN(visibility) AS minVis,
           MAX(visibility) AS maxVis,
           AVG(apparent_temperature) AS avgAppTemp,
           MIN(apparent_temperature) AS minAppTemp,
           MAX(apparent_temperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(wind_speed) AS avgWSpeed,
           MIN(wind_speed) AS minWSpeed,
           MAX(wind_speed) AS maxWSpeed,
           AVG(wind_bearing) AS avgWBear,
           MIN(wind_bearing) AS minWBear,
           MAX(wind_bearing) AS maxWBear,
           AVG(precip_intensity) AS avgPrec,
           MIN(precip_intensity) AS minPrec,
           MAX(precip_intensity) AS maxPrec,
           AVG(dew_point) AS avgDPoint,
           MIN(dew_point) AS minDPoint,
           MAX(dew_point) AS maxDPoint
    FROM df
""")

weather_month_df = spark.sql("""
    SELECT MONTH(time) AS month,
           AVG(temperature) AS avgTemp,
           MIN(temperature) AS minTemp,
           MAX(temperature) AS maxTemp,
           AVG(humidity) AS avgHum,
           MIN(humidity) AS minHum,
           MAX(humidity) AS maxHum,
           AVG(visibility) AS avgVis,
           MIN(visibility) AS minVis,
           MAX(visibility) AS maxVis,
           AVG(apparent_temperature) AS avgAppTemp,
           MIN(apparent_temperature) AS minAppTemp,
           MAX(apparent_temperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(wind_speed) AS avgWSpeed,
           MIN(wind_speed) AS minWSpeed,
           MAX(wind_speed) AS maxWSpeed,
           AVG(wind_bearing) AS avgWBear,
           MIN(wind_bearing) AS minWBear,
           MAX(wind_bearing) AS maxWBear,
           AVG(precip_intensity) AS avgPrec,
           MIN(precip_intensity) AS minPrec,
           MAX(precip_intensity) AS maxPrec,
           AVG(dew_point) AS avgDPoint,
           MIN(dew_point) AS minDPoint,
           MAX(dew_point) AS maxDPoint
    FROM df
    GROUP BY MONTH(time)
""")
weather_dayofweek_df = spark.sql("""
    SELECT DAYOFWEEK(time) AS day_of_week,
           AVG(temperature) AS avgTemp,
           MIN(temperature) AS minTemp,
           MAX(temperature) AS maxTemp,
           AVG(humidity) AS avgHum,
           MIN(humidity) AS minHum,
           MAX(humidity) AS maxHum,
           AVG(visibility) AS avgVis,
           MIN(visibility) AS minVis,
           MAX(visibility) AS maxVis,
           AVG(apparent_temperature) AS avgAppTemp,
           MIN(apparent_temperature) AS minAppTemp,
           MAX(apparent_temperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(wind_speed) AS avgWSpeed,
           MIN(wind_speed) AS minWSpeed,
           MAX(wind_speed) AS maxWSpeed,
           AVG(wind_bearing) AS avgWBear,
           MIN(wind_bearing) AS minWBear,
           MAX(wind_bearing) AS maxWBear,
           AVG(precip_intensity) AS avgPrec,
           MIN(precip_intensity) AS minPrec,
           MAX(precip_intensity) AS maxPrec,
           AVG(dew_point) AS avgDPoint,
           MIN(dew_point) AS minDPoint,
           MAX(dew_point) AS maxDPoint
    FROM df
    GROUP BY DAYOFWEEK(time)
""")
weather_day_df = spark.sql("""
    SELECT DAYOFYEAR(time) AS day,
           AVG(temperature) AS avgTemp,
           MIN(temperature) AS minTemp,
           MAX(temperature) AS maxTemp,
           AVG(humidity) AS avgHum,
           MIN(humidity) AS minHum,
           MAX(humidity) AS maxHum,
           AVG(visibility) AS avgVis,
           MIN(visibility) AS minVis,
           MAX(visibility) AS maxVis,
           AVG(apparent_temperature) AS avgAppTemp,
           MIN(apparent_temperature) AS minAppTemp,
           MAX(apparent_temperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(wind_speed) AS avgWSpeed,
           MIN(wind_speed) AS minWSpeed,
           MAX(wind_speed) AS maxWSpeed,
           AVG(wind_bearing) AS avgWBear,
           MIN(wind_bearing) AS minWBear,
           MAX(wind_bearing) AS maxWBear,
           AVG(precip_intensity) AS avgPrec,
           MIN(precip_intensity) AS minPrec,
           MAX(precip_intensity) AS maxPrec,
           AVG(dew_point) AS avgDPoint,
           MIN(dew_point) AS minDPoint,
           MAX(dew_point) AS maxDPoint
    FROM df
    GROUP BY DAYOFYEAR(time)
""")

# Ulteriori Analisi??

# Save on cassandra
kw_total_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "kw_use_all_time") \
   .mode("append") \
   .save()

kw_month_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "kw_use_month") \
   .mode("append") \
   .save()

kw_dayofweek_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "kw_use_day_of_week") \
   .mode("append") \
   .save()

kw_day_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "kw_use_day") \
   .mode("append") \
   .save()

weather_total_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_use_all_time") \
   .mode("append") \
   .save()

weather_month_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_use_month") \
   .mode("append") \
   .save()

weather_dayofweek_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_use_day_of_week") \
   .mode("append") \
   .save()

weather_day_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_use_day") \
   .mode("append") \
   .save()

spark.stop()

# kw_total_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_all_time.csv", index = False)
# kw_month_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_month.csv", index = False)
# kw_dayofweek_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_day_of_week.csv", index = False)
# kw_day_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_day.csv", index = False)

# weather_total_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_all_time.csv", index = False)
# weather_month_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_month.csv", index = False)
# weather_dayofweek_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_day_of_week.csv", index = False)
# weather_day_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_day.csv", index = False)

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
    SELECT COUNT(*) AS count,
           AVG(temperature) AS avgtemp,
           MIN(temperature) AS mintemp,
           MAX(temperature) AS maxtemp,
           AVG(humidity) AS avghum,
           MIN(humidity) AS minhum,
           MAX(humidity) AS maxhum,
           AVG(visibility) AS avgvis,
           MIN(visibility) AS minvis,
           MAX(visibility) AS maxvis,
           AVG(apparent_temperature) AS avgapptemp,
           MIN(apparent_temperature) AS minapptemp,
           MAX(apparent_temperature) AS maxapptemp,
           AVG(pressure) AS avgpress,
           MIN(pressure) AS minpress,
           MAX(pressure) AS maxpress,
           AVG(wind_speed) AS avgwspeed,
           MIN(wind_speed) AS minwspeed,
           MAX(wind_speed) AS maxwspeed,
           AVG(wind_bearing) AS avgwbear,
           MIN(wind_bearing) AS minwbear,
           MAX(wind_bearing) AS maxwbear,
           AVG(precip_intensity) AS avgprec,
           MIN(precip_intensity) AS minprec,
           MAX(precip_intensity) AS maxprec,
           AVG(dew_point) AS avgdpoint,
           MIN(dew_point) AS mindpoint,
           MAX(dew_point) AS maxdpoint
    FROM df
""")

weather_month_df = spark.sql("""
    SELECT MONTH(time) AS month,
           AVG(temperature) AS avgtemp,
           MIN(temperature) AS mintemp,
           MAX(temperature) AS maxtemp,
           AVG(humidity) AS avghum,
           MIN(humidity) AS minhum,
           MAX(humidity) AS maxhum,
           AVG(visibility) AS avgvis,
           MIN(visibility) AS minvis,
           MAX(visibility) AS maxvis,
           AVG(apparent_temperature) AS avgapptemp,
           MIN(apparent_temperature) AS minapptemp,
           MAX(apparent_temperature) AS maxapptemp,
           AVG(pressure) AS avgpress,
           MIN(pressure) AS minpress,
           MAX(pressure) AS maxpress,
           AVG(wind_speed) AS avgwspeed,
           MIN(wind_speed) AS minwspeed,
           MAX(wind_speed) AS maxwspeed,
           AVG(wind_bearing) AS avgwbear,
           MIN(wind_bearing) AS minwbear,
           MAX(wind_bearing) AS maxwbear,
           AVG(precip_intensity) AS avgprec,
           MIN(precip_intensity) AS minprec,
           MAX(precip_intensity) AS maxprec,
           AVG(dew_point) AS avgdpoint,
           MIN(dew_point) AS mindpoint,
           MAX(dew_point) AS maxdpoint
    FROM df
    GROUP BY MONTH(time)
""")
weather_dayofweek_df = spark.sql("""
    SELECT DAYOFWEEK(time) AS day_of_week,
           AVG(temperature) AS avgtemp,
           MIN(temperature) AS mintemp,
           MAX(temperature) AS maxtemp,
           AVG(humidity) AS avghum,
           MIN(humidity) AS minhum,
           MAX(humidity) AS maxhum,
           AVG(visibility) AS avgvis,
           MIN(visibility) AS minvis,
           MAX(visibility) AS maxvis,
           AVG(apparent_temperature) AS avgapptemp,
           MIN(apparent_temperature) AS minapptemp,
           MAX(apparent_temperature) AS maxapptemp,
           AVG(pressure) AS avgpress,
           MIN(pressure) AS minpress,
           MAX(pressure) AS maxpress,
           AVG(wind_speed) AS avgwspeed,
           MIN(wind_speed) AS minwspeed,
           MAX(wind_speed) AS maxwspeed,
           AVG(wind_bearing) AS avgwbear,
           MIN(wind_bearing) AS minwbear,
           MAX(wind_bearing) AS maxwbear,
           AVG(precip_intensity) AS avgprec,
           MIN(precip_intensity) AS minprec,
           MAX(precip_intensity) AS maxprec,
           AVG(dew_point) AS avgdpoint,
           MIN(dew_point) AS mindpoint,
           MAX(dew_point) AS maxdpoint
    FROM df
    GROUP BY DAYOFWEEK(time)
""")
weather_day_df = spark.sql("""
    SELECT DAYOFYEAR(time) AS day,
           AVG(temperature) AS avgtemp,
           MIN(temperature) AS mintemp,
           MAX(temperature) AS maxtemp,
           AVG(humidity) AS avghum,
           MIN(humidity) AS minhum,
           MAX(humidity) AS maxhum,
           AVG(visibility) AS avgvis,
           MIN(visibility) AS minvis,
           MAX(visibility) AS maxvis,
           AVG(apparent_temperature) AS avgapptemp,
           MIN(apparent_temperature) AS minapptemp,
           MAX(apparent_temperature) AS maxapptemp,
           AVG(pressure) AS avgpress,
           MIN(pressure) AS minpress,
           MAX(pressure) AS maxpress,
           AVG(wind_speed) AS avgwspeed,
           MIN(wind_speed) AS minwspeed,
           MAX(wind_speed) AS maxwspeed,
           AVG(wind_bearing) AS avgwbear,
           MIN(wind_bearing) AS minwbear,
           MAX(wind_bearing) AS maxwbear,
           AVG(precip_intensity) AS avgprec,
           MIN(precip_intensity) AS minprec,
           MAX(precip_intensity) AS maxprec,
           AVG(dew_point) AS avgdpoint,
           MIN(dew_point) AS mindpoint,
           MAX(dew_point) AS maxdpoint
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
   .option("table", "weather_all_time") \
   .mode("append") \
   .save()

weather_month_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_month") \
   .mode("append") \
   .save()

weather_dayofweek_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_day_of_week") \
   .mode("append") \
   .save()

weather_day_df.write \
   .format("org.apache.spark.sql.cassandra") \
   .option("keyspace", "my_batch") \
   .option("table", "weather_day") \
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

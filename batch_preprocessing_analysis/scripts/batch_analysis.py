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

spark = SparkSession.builder.appName("analysis").getOrCreate()

df = spark.read.option("header", True).csv(input_filepath + '/preprocessed.csv').cache()

df.printSchema()

df.createOrReplaceTempView("df")

# Analisi consumi e generazione [kw]

kw_total_df = spark.sql("""
    SELECT 
        SUM(`use [kW]`) AS `use total [KW]`,
        SUM(`gen [kW]`) AS `gen total [KW]`,
        AVG(`use [kW]`) AS `use avg [KW]`,
        AVG(`gen [kW]`) AS `gen avg [KW]`,
        MAX(`use [kW]`) AS `use max [KW]`,
        MAX(`gen [kW]`) AS `gen max [KW]`,
        MIN(`use [kW]`) AS `use min [KW]`,
        MIN(`gen [kW]`) AS `gen min [KW]`
    FROM df
""")

kw_month_df = spark.sql("""
    SELECT MONTH(time) AS month,
           SUM(`use [kW]`) AS `use total [KW]`,
           SUM(`gen [kW]`) AS `gen total [KW]`,
           AVG(`use [kW]`) AS `use avg [KW]`,
           AVG(`gen [kW]`) AS `gen avg [KW]`,
           MAX(`use [kW]`) AS `use max [KW]`,
           MAX(`gen [kW]`) AS `gen max [KW]`,
           MIN(`use [kW]`) AS `use min [KW]`,
           MIN(`gen [kW]`) AS `gen min [KW]`
    FROM df
    GROUP BY MONTH(time)
""")

kw_dayofweek_df = spark.sql("""
    SELECT DAYOFWEEK(time) AS `day of week`,
           SUM(`use [kW]`) AS `use total [KW]`,
           SUM(`gen [kW]`) AS `gen total [KW]`,
           AVG(`use [kW]`) AS `use avg [KW]`,
           AVG(`gen [kW]`) AS `gen avg [KW]`,
           MAX(`use [kW]`) AS `use max [KW]`,
           MAX(`gen [kW]`) AS `gen max [KW]`,
           MIN(`use [kW]`) AS `use min [KW]`,
           MIN(`gen [kW]`) AS `gen min [KW]`
    FROM df
    GROUP BY DAYOFWEEK(time)
""")


kw_day_df = spark.sql("""
    SELECT DAYOFYEAR(time) AS day,
           SUM(`use [kW]`) AS `use total [KW]`,
           SUM(`gen [kW]`) AS `gen total [KW]`,
           AVG(`use [kW]`) AS `use avg [KW]`,
           AVG(`gen [kW]`) AS `gen avg [KW]`,
           MAX(`use [kW]`) AS `use max [KW]`,
           MAX(`gen [kW]`) AS `gen max [KW]`,
           MIN(`use [kW]`) AS `use min [KW]`,
           MIN(`gen [kW]`) AS `gen min [KW]`
    FROM df
    GROUP BY DAYOFYEAR(time)
""")

#kw_total_df.show()
#kw_month_df.show()
#kw_day_df.show()
#kw_dayofweek_df.show()

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
           AVG(apparentTemperature) AS avgAppTemp,
           MIN(apparentTemperature) AS minAppTemp,
           MAX(apparentTemperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(windSpeed) AS avgWSpeed,
           MIN(windSpeed) AS minWSpeed,
           MAX(windSpeed) AS maxWSpeed,
           AVG(windBearing) AS avgWBear,
           MIN(windBearing) AS minWBear,
           MAX(windBearing) AS maxWBear,
           AVG(precipIntensity) AS avgPrec,
           MIN(precipIntensity) AS minPrec,
           MAX(precipIntensity) AS maxPrec,
           AVG(dewPoint) AS avgDPoint,
           MIN(dewPoint) AS minDPoint,
           MAX(dewPoint) AS maxDPoint
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
           AVG(apparentTemperature) AS avgAppTemp,
           MIN(apparentTemperature) AS minAppTemp,
           MAX(apparentTemperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(windSpeed) AS avgWSpeed,
           MIN(windSpeed) AS minWSpeed,
           MAX(windSpeed) AS maxWSpeed,
           AVG(windBearing) AS avgWBear,
           MIN(windBearing) AS minWBear,
           MAX(windBearing) AS maxWBear,
           AVG(precipIntensity) AS avgPrec,
           MIN(precipIntensity) AS minPrec,
           MAX(precipIntensity) AS maxPrec,
           AVG(dewPoint) AS avgDPoint,
           MIN(dewPoint) AS minDPoint,
           MAX(dewPoint) AS maxDPoint
    FROM df
    GROUP BY MONTH(time)
""")
weather_dayofweek_df = spark.sql("""
    SELECT DAYOFWEEK(time) AS `day of week`,
           AVG(temperature) AS avgTemp,
           MIN(temperature) AS minTemp,
           MAX(temperature) AS maxTemp,
           AVG(humidity) AS avgHum,
           MIN(humidity) AS minHum,
           MAX(humidity) AS maxHum,
           AVG(visibility) AS avgVis,
           MIN(visibility) AS minVis,
           MAX(visibility) AS maxVis,
           AVG(apparentTemperature) AS avgAppTemp,
           MIN(apparentTemperature) AS minAppTemp,
           MAX(apparentTemperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(windSpeed) AS avgWSpeed,
           MIN(windSpeed) AS minWSpeed,
           MAX(windSpeed) AS maxWSpeed,
           AVG(windBearing) AS avgWBear,
           MIN(windBearing) AS minWBear,
           MAX(windBearing) AS maxWBear,
           AVG(precipIntensity) AS avgPrec,
           MIN(precipIntensity) AS minPrec,
           MAX(precipIntensity) AS maxPrec,
           AVG(dewPoint) AS avgDPoint,
           MIN(dewPoint) AS minDPoint,
           MAX(dewPoint) AS maxDPoint
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
           AVG(apparentTemperature) AS avgAppTemp,
           MIN(apparentTemperature) AS minAppTemp,
           MAX(apparentTemperature) AS maxAppTemp,
           AVG(pressure) AS avgPress,
           MIN(pressure) AS minPress,
           MAX(pressure) AS maxPress,
           AVG(windSpeed) AS avgWSpeed,
           MIN(windSpeed) AS minWSpeed,
           MAX(windSpeed) AS maxWSpeed,
           AVG(windBearing) AS avgWBear,
           MIN(windBearing) AS minWBear,
           MAX(windBearing) AS maxWBear,
           AVG(precipIntensity) AS avgPrec,
           MIN(precipIntensity) AS minPrec,
           MAX(precipIntensity) AS maxPrec,
           AVG(dewPoint) AS avgDPoint,
           MIN(dewPoint) AS minDPoint,
           MAX(dewPoint) AS maxDPoint
    FROM df
    GROUP BY DAYOFYEAR(time)
""")
                           
# weather_total_df.show()
# weather_month_df.show()
# weather_dayofweek_df.show()
# weather_day_df.show()

# kw_total_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_all_time.csv", index = False)
# kw_month_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_month.csv", index = False)
# kw_dayofweek_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_day_of_week.csv", index = False)
# kw_day_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/kW_use_day.csv", index = False)

# weather_total_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_all_time.csv", index = False)
# weather_month_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_month.csv", index = False)
# weather_dayofweek_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_day_of_week.csv", index = False)
# weather_day_df.toPandas().to_csv("/home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_day.csv", index = False)

# Ulteriori Analisi??

# Save on cassandra
import pandas as pd
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, month, avg, year, dayofweek, count, when, sum, last_day, next_day, dayofyear, \
    dayofmonth, datediff, row_number, lit, max

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

kw_month_df = df.groupBy(month('time').alias('month')).agg(sum('use [kW]').alias('use total [KW]'), 
                                                           sum('gen [kW]').alias('gen total [KW]'))
# kw_dayofweek_df = 
# kw_day_df = 
kw_total_df.show()
kw_month_df.show()
# Analisi tempo atmosferico

# weather_total_df = 
# weather_month_df = 
# weather_dayofweek_df = 
# weather_day_df = 

# Ulteriori Analisi??

# sum('use [kW]').alias('use total [KW]'), sum('gen [kW]').alias('gen total [KW]'),
#                         avg('use [kW]').alias('use avg [KW]'), avg('gen [kW]').alias('gen avg [KW]'),
#                         max('use [kW]').alias('use max [KW]'), max('gen [kW]').alias('gen max [KW]'),
#                         min('use [kW]').alias('use max [KW]'), min('gen [kW]').alias('gen max [KW]')
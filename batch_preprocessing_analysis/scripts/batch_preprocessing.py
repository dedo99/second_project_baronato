from pyspark.sql import SparkSession
import pandas as pd
import datetime
import argparse
import re
from datetime import timedelta

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

args = parser.parse_args()
input_filepath = args.input_path

# spark = SparkSession.builder.appName("PREPROCESSING").getOrCreate()

spark = SparkSession.builder \
    .appName('PREPROCESSING') \
    .config('spark.cassandra.connection.host', 'cassandra') \
    .config('spark.cassandra.connection.port', '9042') \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

indicies = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 28, 29, 30, 31]

pattern = r"[-+]?\d+(?:\.\d+)?[eE][-+]?\d+"

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = input_RDD.map(lambda line: line.split(','))

# Elimina gli attributi "icon" e "cloudCover"
delete_columns_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

def convert_temperature(line):
    try:
        temp_app = float(line[23])
        temp = float(line[19])
        line[19] = (temp - 32) * 5/9
        line[23] = (temp_app - 32) * 5/9
        return line
    except ValueError:
        return line
    
# Conversione da Fahrenheit a Celsius
convert_temperature_RDD = delete_columns_RDD.map(convert_temperature)

# Rimuovi notazione scientifica
def convert_scientific_notation(line):
    for i in range(1, len(line)):
        if re.fullmatch(pattern, str(line[i])):
            y = float(line[i])
            line[i] = "{:f}".format(y)
    return line

scientific_notation_RDD = convert_temperature_RDD.map(convert_scientific_notation)

header = scientific_notation_RDD.first()

df_spark = scientific_notation_RDD.toDF()

df = df_spark.toPandas()

df = df.iloc[1:]

df.columns = header

print('\n\n' + str(len(df)) + '\n\n')

# df.to_csv('/home/pietro/Documenti/BigData/second_project_baronato/datasets/preprocessed.csv', index=False)

# Save on Cassandra

df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "my_batch") \
    .option("table", "preprocessed_row_dataset") \
    .mode("append") \
    .save()

spark.stop()
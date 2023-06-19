from pyspark.sql import SparkSession
import pandas as pd
import datetime
import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path
#input_filepath = args.input_path

spark = SparkSession.builder.appName("PREPROCESSING").getOrCreate()

indicies = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 28, 29, 30]

data = 0

pattern = r"[-+]?\d+(?:\.\d+)?[eE][-+]?\d+"

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = input_RDD.map(lambda line: line.split(','))

# Elimina gli attributi "icon" e "cloudCover"
delete_columns_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

def convert_temperature(line):
    try:
        temp = int(line[19])
        line[19] = (temp - 32) * 5/9
        return line
    except ValueError:
        return line
    
# Conversione da Fahrenheit a Celsius
convert_temperature_RDD = delete_columns_RDD.map(convert_temperature)

def convert_time(line):
    global data
    try:
        unix_date = int(line[0]) + data
        data = unix_date
        line[0] = datetime.datetime.fromtimestamp(unix_date).strftime('%Y-%m-%d %H:%M:%S')
        return line
    except ValueError:
        return line

convert_time_RDD = convert_temperature_RDD.map(convert_time)

def convert_scientific_notation(line):
    for i in range(0, len(line)):
        if re.fullmatch(pattern, str(line[i])):
            y = float(line[i])
            line[i] = "{:f}".format(y)
    return line

scientific_notation_RDD = convert_time_RDD.map(convert_scientific_notation)

print(scientific_notation_RDD.collect())

scientific_notation_RDD.saveAsTextFile(output_filepath)

#df = pd.DataFrame(scientific_notation_RDD.collect())

#df.to_csv("input/output_preprocessing.csv", index = False)
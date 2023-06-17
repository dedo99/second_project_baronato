from pyspark.sql import SparkSession
import pandas as pd
import pyspark.pandas as ps
from datetime import timedelta
import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path
#input_filepath = args.input_path

# read csv
df = ps.read_csv(input_filepath)

# Elimina gli attributi "icon" e "cloudCover"
df = df.drop(['icon', 'cloudCover'], axis=1)

# Conversione da Fahrenheit a Celsius
df['temperature'] = (df['temperature'] - 32) * 5/9

# Conversione dell'attributo "time" da UNIX time a data incrementando di 60 secondi
start_time = pd.to_datetime(df['time'].iloc[0], unit='s')
df['time'] = start_time

for i in range(1, len(df)):
    df.loc[i, 'time'] = df.loc[i-1, 'time'] + timedelta(seconds=60)

# Conversione notazione scientifica in float
# pattern = r"[-+]?\d+(?:\.\d+)?[eE][-+]?\d+"
# for i in range(1, len(df)):
#     for column in df.columns:
#         if re.fullmatch(pattern, str(df.loc[i, column])):
#             y = float(df.loc[i, column])
#             df.loc[i, column] = "{:f}".format(y)

# Visualizza il DataFrame preprocessato
print(df.head)

df.to_csv(output_filepath + '/preprocessed.csv')

# def batch_processing_function():
#     print("Batch Processing")
from batch_preprocessing import batch_processing_function
from batch_analysis import batch_analysis_function
from streaming import streaming_function
from consumer import consumer
import time
from pyspark.sql import SparkSession
import argparse
import pandas as pd
import pyspark.pandas as ps

#psdf2 = ps.DataFrame({'id': [5, 4, 3]})

#print(psdf2)
# parser = argparse.ArgumentParser()
# parser.add_argument("--input_path", type=str, help="Input file path")
# args = parser.parse_args()
# input_filepath = args.input_path

# spark = SparkSession.builder.appName("FIRST_JOB").getOrCreate()

# input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# first_ten = input_RDD.filter(lambda row: row[0]>= 1451624400 and row[0]<= 1451624410)

# print(input_RDD.collect())

batch_processing_function()

batch_analysis_function()

#consumer()

# time.sleep(120)

print('Ciao a tutti!')
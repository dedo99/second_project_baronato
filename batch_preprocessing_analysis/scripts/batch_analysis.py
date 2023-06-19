import pandas as pd
import logging
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
args = parser.parse_args()
input_filepath = args.input_path

spark = SparkSession.builder.appName("analisis").getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

logging.info(input_RDD.collect())
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

spark = SparkSession.builder.appName("analisis").getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).cache()

logger.info(len(input_RDD.collect()))
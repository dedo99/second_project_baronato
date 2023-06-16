from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import logging

# Ottieni il logger del modulo kafka
logger = logging.getLogger("kafka")
logger.setLevel(logging.DEBUG)  # Imposta il livello di logging desiderato

# Configura l'handler di logging per il logger di kafka
handler = logging.StreamHandler()  # Puoi modificare questo per inoltrare i log a un file
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


#initialize SparkSession
spark = SparkSession \
    .builder \
    .appName('Streaming') \
    .getOrCreate()

lines_DF = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9092') \
    .option('subscribe', 'my-topic') \
    .load()

rows_DF = lines_DF.map(lambda line: line.split(','))

query = rows_DF \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()

query.awaitTermination()
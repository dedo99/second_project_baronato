from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'


# Configura le informazioni di connessione a Kafka
kafka_params = {
    'kafka.bootstrap.servers': 'localhost:29092',  # Modifica se Kafka è in esecuzione su un altro indirizzo
    'subscribe': 'my-topic',  # Specifica il topic da consumare
    'startingOffsets': 'earliest'  # Inizia a leggere dal primo messaggio disponibile
}

# Crea una sessione Spark
spark = SparkSession.builder \
    .appName('KafkaStreamingConsumer') \
    .getOrCreate()

# Leggi i dati da Kafka utilizzando la libreria spark-kafka-connector
df = spark \
    .readStream \
    .format('kafka') \
    .options(**kafka_params) \
    .load()

# Elabora i messaggi dal dataframe
query = df \
    .writeStream \
    .format('console') \
    .start()

# Attendi la terminazione dello streaming
query.awaitTermination()

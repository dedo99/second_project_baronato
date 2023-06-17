from pyspark.sql import SparkSession


# Configura le informazioni di connessione a Kafka
kafka_params = {
    'kafka.bootstrap.servers': 'kafka:9092',  # Modifica se Kafka è in esecuzione su un altro indirizzo
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
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format('console') \
    .start()

# Attendi la terminazione dello streaming
query.awaitTermination()

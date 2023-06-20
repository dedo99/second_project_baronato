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
    .config('spark.cassandra.connection.host', 'cassandra') \
    .config('spark.cassandra.connection.port', '9042') \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()


# Leggi i dati da Kafka utilizzando la libreria spark-kafka-connector
df = spark \
    .readStream \
    .format('kafka') \
    .options(**kafka_params) \
    .load()


# Elabora i messaggi dal dataframe
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Scrivi i messaggi in Cassandra
df.writeStream \
    .format('org.apache.spark.sql.cassandra') \
    .option('keyspace', 'streaming') \
    .option('table', 'raw_row') \
    .start()

# Attendi la terminazione dello streaming
spark.streams.awaitAnyTermination()

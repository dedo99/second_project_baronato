from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Configura le informazioni di connessione a Kafka
kafka_params = {
    'kafka.bootstrap.servers': 'kafka:9092',
    'subscribe': 'my-topic',
    'startingOffsets': 'earliest'
}

# Configura le informazioni di connessione a InfluxDB
token = os.environ.get("INFLUXDB_TOKEN")
influxdb_url = "http://influxdb:8086"
influxdb_org = "iothome"
influxdb_bucket = "iothome_bucket"

# Crea una sessione Spark
spark = SparkSession.builder \
    .appName('KafkaStreamingConsumer') \
    .config('spark.cassandra.connection.host', 'cassandra') \
    .config('spark.cassandra.connection.port', '9042') \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# Crea la connessione a InfluxDB
client = InfluxDBClient(url=influxdb_url, token=token, org=influxdb_org)

# Funzione per scrivere i dati su InfluxDB
def write_to_influxdb(row):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    try:
        timestamp_unix_str = row.key
        # Converti il timestamp Unix in un oggetto datetime
        timestamp_unix = int(timestamp_unix_str)
        timestamp_obj = datetime.fromtimestamp(timestamp_unix)
        timestamp_str = timestamp_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        point = Point("raw_iothome_data")
        point.tag("date", timestamp_str)  # Aggiungi il campo 'date' come tag
        point.field("measurement", row.value)
        print("scritturaaaaa!!")
        write_api.write(bucket=influxdb_bucket, org="iothome", record=point)
    except ValueError:
        # Gestisci il caso in cui il valore del timestamp non sia valido
        pass


# Leggi i dati da Kafka
df = spark.readStream \
    .format('kafka') \
    .options(**kafka_params) \
    .load()

# Elabora i messaggi dal dataframe
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.withColumn("value_array", split(col("value"), ","))
df = df.withColumn("key", col("value_array").getItem(0))
df = df.withColumn("value", col("value_array").getItem(1))
df = df.drop("value_array")


# Visualizza lo streaming dei dati
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# # Scrivi i messaggi in Cassandra
query_cassandra = df.writeStream \
    .format('org.apache.spark.sql.cassandra') \
    .option('keyspace', 'streaming') \
    .option('table', 'raw_row_dataset') \
    .option('checkpointLocation', 'checkpoint') \
    .start()

# Scrivi i messaggi in InfluxDB
query_influx = df.writeStream \
    .foreach(write_to_influxdb) \
    .start()

# Attendi la terminazione dello streaming
# spark.streams.awaitAnyTermination()
query.awaitTermination()
query_cassandra.awaitTermination()
query_influx.awaitTermination()
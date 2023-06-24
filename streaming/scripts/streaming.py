from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, format_number
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# --------------------------------------------------------------------------------
# ------------------------------CONFIGURATION DB----------------------------------
# --------------------------------------------------------------------------------

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

# --------------------------------------------------------------------------------
# ------------------------------AUXILIARY FUNCTION--------------------------------
# --------------------------------------------------------------------------------

# Funzione per scrivere i dati su InfluxDB
def write_to_influxdb(row):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    try:
        timestamp_unix_str = row.time_key
        # Converti il timestamp Unix in un oggetto datetime
        timestamp_unix = int(timestamp_unix_str)
        timestamp_obj = datetime.fromtimestamp(timestamp_unix)
        timestamp_str = timestamp_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        point = Point("raw_iothome_data")
        point.tag("date", timestamp_str)  # Aggiungi il campo 'date' come tag
        point.field("use_kw", row.use_kw)
        point.field("gen_kw", row.gen_kw)
        point.field("house_overall_kw", row.house_overall_kw)
        point.field("dishwasher_kw", row.dishwasher_kw)
        point.field("furnace_1_kw", row.furnace_1_kw)
        point.field("furnace_2_kw", row.furnace_2_kw)
        point.field("home_office_kw", row.home_office_kw)
        point.field("fridge_kw", row.fridge_kw)
        point.field("wine_cellar_kw", row.wine_cellar_kw)
        point.field("garage_door_kw", row.garage_door_kw)
        point.field("kitchen_12_kw", row.kitchen_12_kw)
        point.field("kitchen_14_kw", row.kitchen_14_kw)
        point.field("kitchen_38_kw", row.kitchen_38_kw)
        point.field("barn_kw", row.barn_kw)
        point.field("well_kw", row.well_kw)
        point.field("microwave_kw", row.microwave_kw)
        point.field("living_room_kw", row.living_room_kw)
        point.field("solar_kw", row.solar_kw)
        point.field("temperature", row.temperature)
        point.field("icon", row.icon)
        point.field("humidity", row.humidity)
        point.field("visibility", row.visibility)
        point.field("summary", row.summary)
        point.field("apparent_temperature", row.apparent_temperature)
        point.field("pressure", row.pressure)
        point.field("wind_speed", row.wind_speed)
        point.field("cloud_cover", row.cloud_cover)
        point.field("wind_bearing", row.wind_bearing)
        point.field("precip_intensity", row.precip_intensity)
        point.field("dew_point", row.dew_point)
        point.field("precip_probability", row.precip_probability)

        print("scritturaaaaa!!")
        write_api.write(bucket=influxdb_bucket, org="iothome", record=point)
    except ValueError:
        # Gestisci il caso in cui il valore del timestamp non sia valido
        pass


# --------------------------------------------------------------------------------
# ---------------------------READING FROM KAFKA-----------------------------------
# --------------------------------------------------------------------------------

# Leggi i dati da Kafka
df = spark.readStream \
    .format('kafka') \
    .options(**kafka_params) \
    .load()

# Elabora i messaggi dal dataframe
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.withColumn("value_array", split(col("value"), ","))

df = df.withColumn("time_key", col("value_array").getItem(0))
df = df.withColumn("use_kw", col("value_array").getItem(1))
df = df.withColumn("gen_kw", col("value_array").getItem(2))
df = df.withColumn("house_overall_kw", col("value_array").getItem(3))
df = df.withColumn("dishwasher_kw", col("value_array").getItem(4))
df = df.withColumn("furnace_1_kw", col("value_array").getItem(5))
df = df.withColumn("furnace_2_kw", col("value_array").getItem(6))
df = df.withColumn("home_office_kw", col("value_array").getItem(7))
df = df.withColumn("fridge_kw", col("value_array").getItem(8))
df = df.withColumn("wine_cellar_kw", col("value_array").getItem(9))
df = df.withColumn("garage_door_kw", col("value_array").getItem(10))
df = df.withColumn("kitchen_12_kw", col("value_array").getItem(11))
df = df.withColumn("kitchen_14_kw", col("value_array").getItem(12))
df = df.withColumn("kitchen_38_kw", col("value_array").getItem(13))
df = df.withColumn("barn_kw", col("value_array").getItem(14))
df = df.withColumn("well_kw", col("value_array").getItem(15))
df = df.withColumn("microwave_kw", col("value_array").getItem(16))
df = df.withColumn("living_room_kw", col("value_array").getItem(17))
df = df.withColumn("solar_kw", col("value_array").getItem(18))
df = df.withColumn("temperature", col("value_array").getItem(19))
df = df.withColumn("icon", col("value_array").getItem(20))
df = df.withColumn("humidity", col("value_array").getItem(21))
df = df.withColumn("visibility", col("value_array").getItem(22))
df = df.withColumn("summary", col("value_array").getItem(23))
df = df.withColumn("apparent_temperature", col("value_array").getItem(24))
df = df.withColumn("pressure", col("value_array").getItem(25))
df = df.withColumn("wind_speed", col("value_array").getItem(26))
df = df.withColumn("cloud_cover", col("value_array").getItem(27))
df = df.withColumn("wind_bearing", col("value_array").getItem(28))
df = df.withColumn("precip_intensity", col("value_array").getItem(29))
df = df.withColumn("dew_point", col("value_array").getItem(30))
df = df.withColumn("precip_probability", col("value_array").getItem(31))

df = df.drop("value_array")
df = df.drop("key")
df = df.drop("value")

# --------------------------------------------------------------------------------
# ------------------------------PREPROCESSING-------------------------------------
# --------------------------------------------------------------------------------

# Converti la colonna da "STRING" a "NUMERIC"
df = df.withColumn("temperature", col("temperature").cast("double")) 
df = df.withColumn("apparent_temperature", col("apparent_temperature").cast("double")) 
df = df.withColumn("use_kw", col("use_kw").cast("double")) 
df = df.withColumn("gen_kw", col("gen_kw").cast("double")) 
df = df.withColumn("house_overall_kw", col("house_overall_kw").cast("double")) 
df = df.withColumn("dishwasher_kw", col("dishwasher_kw").cast("double")) 
df = df.withColumn("furnace_1_kw", col("furnace_1_kw").cast("double")) 
df = df.withColumn("furnace_2_kw", col("furnace_2_kw").cast("double")) 
df = df.withColumn("home_office_kw", col("home_office_kw").cast("double")) 
df = df.withColumn("fridge_kw", col("fridge_kw").cast("double")) 
df = df.withColumn("wine_cellar_kw", col("wine_cellar_kw").cast("double")) 
df = df.withColumn("garage_door_kw", col("garage_door_kw").cast("double")) 
df = df.withColumn("kitchen_12_kw", col("kitchen_12_kw").cast("double")) 
df = df.withColumn("kitchen_14_kw", col("kitchen_14_kw").cast("double")) 
df = df.withColumn("kitchen_38_kw", col("kitchen_38_kw").cast("double")) 
df = df.withColumn("barn_kw", col("barn_kw").cast("double")) 
df = df.withColumn("well_kw", col("well_kw").cast("double")) 
df = df.withColumn("microwave_kw", col("microwave_kw").cast("double")) 
df = df.withColumn("living_room_kw", col("living_room_kw").cast("double")) 
df = df.withColumn("solar_kw", col("solar_kw").cast("double"))
df = df.withColumn("temperature", col("temperature").cast("double"))
df = df.withColumn("humidity", col("humidity").cast("double"))
df = df.withColumn("visibility", col("visibility").cast("double"))
df = df.withColumn("apparent_temperature", col("apparent_temperature").cast("double"))
df = df.withColumn("pressure", col("pressure").cast("double"))
df = df.withColumn("wind_speed", col("wind_speed").cast("double"))
df = df.withColumn("wind_bearing", col("wind_bearing").cast("double"))
df = df.withColumn("precip_intensity", col("precip_intensity").cast("double"))
df = df.withColumn("dew_point", col("dew_point").cast("double"))
df = df.withColumn("precip_probability", col("precip_probability").cast("double"))

# Converti temperature da Fahrenheit a Celsius
df = df.withColumn("temperature", (col("temperature") - 32) * 5 / 9)

# Converti apparent_temperature da Fahrenheit a Celsius
df = df.withColumn("apparent_temperature", (col("apparent_temperature") - 32) * 5 / 9)

# Rimuovi la notazione scientifica
df = df.withColumn("use_kw", format_number(col("use_kw"), 8))
df = df.withColumn("gen_kw", format_number(col("gen_kw"), 8))
df = df.withColumn("house_overall_kw", format_number(col("house_overall_kw"), 8))
df = df.withColumn("dishwasher_kw", format_number(col("dishwasher_kw"), 8))
df = df.withColumn("furnace_1_kw", format_number(col("furnace_1_kw"), 8))
df = df.withColumn("furnace_2_kw", format_number(col("furnace_2_kw"), 8))
df = df.withColumn("home_office_kw", format_number(col("home_office_kw"), 8))
df = df.withColumn("fridge_kw", format_number(col("fridge_kw"), 8))
df = df.withColumn("wine_cellar_kw", format_number(col("wine_cellar_kw"), 8))
df = df.withColumn("garage_door_kw", format_number(col("garage_door_kw"), 8))
df = df.withColumn("kitchen_12_kw", format_number(col("kitchen_12_kw"), 8))
df = df.withColumn("kitchen_14_kw", format_number(col("kitchen_14_kw"), 8))
df = df.withColumn("kitchen_38_kw", format_number(col("kitchen_38_kw"), 8))
df = df.withColumn("barn_kw", format_number(col("barn_kw"), 8))
df = df.withColumn("well_kw", format_number(col("well_kw"), 8))
df = df.withColumn("microwave_kw", format_number(col("microwave_kw"), 8))
df = df.withColumn("living_room_kw", format_number(col("living_room_kw"), 8))
df = df.withColumn("solar_kw", format_number(col("solar_kw"), 8))



# --------------------------------------------------------------------------------
# ------------------------------STORAGE ON DB-------------------------------------
# --------------------------------------------------------------------------------



# Visualizza lo streaming dei dati
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# # # Scrivi i messaggi in Cassandra
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
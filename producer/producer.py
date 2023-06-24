import logging
import csv
from kafka import KafkaProducer
import time

# Ottieni il logger del modulo kafka
logger = logging.getLogger("kafka")
logger.setLevel(logging.DEBUG)  # Imposta il livello di logging desiderato

# Configura l'handler di logging per il logger di kafka
handler = logging.StreamHandler()  # Puoi modificare questo per inoltrare i log a un file
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Configura il producer Kafka
producer = KafkaProducer(bootstrap_servers='kafka:9092')

# # Invia i dati in batch
# def send_batch_data(topic, data):
#     for item in data:
#         value=item.encode('utf-8')
#         print(value)
#         logger.info(value)
#         producer.send(topic, value)
#     producer.flush()

# # Esempio di invio in batch
# batch_data = ['Data, 1', 'Data, 2', 'Data, 3']
# send_batch_data('my-topic', batch_data)

# Invia un singolo dato
def send_single_data(topic, data):
    value=data.encode('utf-8')
    print(value)
    logger.info(data)
    producer.send(topic, value)


# Invia il dataset a Kafka
def send_dataset_to_kafka(topic, file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Salta l'intestazione del file CSV
        for row in reader:
            data = ','.join(row)  # Unisci gli attributi separati da virgola in una stringa
            send_single_data(topic, data)
            time.sleep(1)

# Esegui l'invio del dataset a Kafka
send_dataset_to_kafka('my-topic', '/input/HomeC2.csv')

# Chiudi il producer
producer.close()

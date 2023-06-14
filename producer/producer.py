import logging
from kafka import KafkaProducer

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

# Invia i dati in batch
def send_batch_data(topic, data):
    for item in data:
        value=item.encode('utf-8')
        print(value)
        logger.info(value)
        producer.send(topic, value)
    producer.flush()

# Invia un singolo dato
def send_single_data(topic, data):
    value=data.encode('utf-8')
    print(value)
    logger.info(data)
    producer.send(topic, value)

# Esempio di invio in batch
batch_data = ['Data 1', 'Data 2', 'Data 3']
send_batch_data('my-topic', batch_data)

# Esempio di invio singolo
single_data = 'Data 4'
send_single_data('my-topic', single_data)

print("CIAO")

# Chiudi il producer
producer.close()

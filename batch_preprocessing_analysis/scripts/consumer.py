import logging
from kafka import KafkaConsumer

# Ottieni il logger del modulo kafka
logger = logging.getLogger("kafka")
logger.setLevel(logging.DEBUG)  # Imposta il livello di logging desiderato

# Configura l'handler di logging per il logger di kafka
handler = logging.StreamHandler()  # Puoi modificare questo per inoltrare i log a un file
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def consumer():
    # Configura il consumer Kafka
    consumer = KafkaConsumer('my-topic', bootstrap_servers='kafka:9092')

    # Leggi i messaggi dal topic
    for message in consumer:
        data = message.value.decode('utf-8')
        print(data)
        logger.info(data)

    # Chiudi il consumer
    consumer.close()

    print("CIAO")

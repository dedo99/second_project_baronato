from kafka import KafkaProducer

# Configura il producer Kafka
producer = KafkaProducer(bootstrap_servers='localhost:29092')

# Invia i dati in batch
def send_batch_data(topic, data):
    for item in data:
        value=item.encode('utf-8')
        print(value)
        producer.send(topic, value)
    producer.flush()

# Invia un singolo dato
def send_single_data(topic, data):
    value=data.encode('utf-8')
    print(value)
    producer.send(topic, value)

# Esempio di invio in batch
batch_data = ['Data, 1', 'Data, 2', 'Data, 3']
send_batch_data('my-topic', batch_data)

# Esempio di invio singolo
single_data = 'Data 4'
send_single_data('my-topic', single_data)

print("CIAO")

# Chiudi il producer
producer.close()

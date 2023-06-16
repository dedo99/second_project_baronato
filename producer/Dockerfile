FROM python:3.9

# WORKDIR /app

# Copia il tuo script producer.py nella directory di lavoro dell'immagine
COPY producer.py .

# Installa le dipendenze necessarie
#RUN pip install kafka-python && apt-get update && apt-get install -y netcat
RUN pip install kafka-python && pip install confluent-kafka 

# Imposta il comando di avvio per il produttore Kafka
CMD ["python", "producer.py"]

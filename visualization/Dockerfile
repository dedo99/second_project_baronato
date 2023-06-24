FROM python:3.9

# WORKDIR /app

# Copia il tuo script producer.py nella directory di lavoro dell'immagine
COPY visualization.py .

EXPOSE 8051

# Installa le dipendenze necessarie
RUN pip install pandas && pip install numpy && pip install matplotlib && pip install streamlit

# Imposta il comando di avvio per il produttore Kafka
CMD streamlit run visualization.py
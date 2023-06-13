FROM bitnami/spark

# Copia gli script Python nella directory dell'applicazione Spark
COPY ./scripts /app

# Avvia l'applicazione Spark
CMD spark-submit --master local[*] /app/main.py --input_path file:///input/HomeC.csv

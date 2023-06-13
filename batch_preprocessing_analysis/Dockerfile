FROM bitnami/spark

RUN pip install pandas && pip install pyarrow

# Copia gli script Python nella directory dell'applicazione Spark
COPY ./scripts /app

# Avvia l'applicazione Spark
CMD spark-submit --master local[*] /app/main.py --input_path file:///input/HomeC.csv

FROM bitnami/spark

RUN pip install pandas && pip install pyarrow && pip install kafka-python 

# USER root

# RUN apt-get update && apt-get install -y netcat

# Copia gli script Python nella directory dell'applicazione Spark
COPY ./scripts /app

CMD ["/app/run_commands.sh"]

# Avvia l'applicazione Spark
#CMD spark-submit --master local[*] /app/batch_preprocessing.py --input_path file:///input/HomeC.csv

#CMD spark-submit --master local[*] /app/batch_analysis.py --input_path file:///input/preprocessed.csv

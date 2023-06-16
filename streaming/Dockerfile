FROM bitnami/spark

RUN pip install kafka-python 

COPY ./scripts /app

CMD spark-submit --master local[*] /app/streaming.py --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
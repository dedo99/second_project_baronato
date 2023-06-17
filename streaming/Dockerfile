FROM bitnami/spark

RUN pip install kafka-python 

COPY ./scripts .

CMD spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --repositories https://repo1.maven.org/maven2 streaming.py
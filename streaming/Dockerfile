FROM bitnami/spark

RUN pip install kafka-python && pip install influxdb-client

ENV INFLUXDB_TOKEN=m_q2yxLk87PUPgYmI9edypxMKyOKeY4hTbByYCw-HyHxbrD_HAGMQbVlLoStQGzUazXddtu50yuvMS_ywsMIqg==

COPY ./scripts .

# Crea la cartella di checkpoint
RUN mkdir checkpoint

CMD spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.7 --repositories https://repo1.maven.org/maven2,https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/ streaming.py
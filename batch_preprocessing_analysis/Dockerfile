FROM bitnami/spark

RUN pip install pandas && pip install pyarrow && pip install kafka-python

# Copia gli script Python nella directory dell'applicazione Spark
COPY ./scripts /app

CMD ["/app/run_commands.sh"]

FROM cassandra:latest

COPY init.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init.sh

CMD ["cassandra", "-f"]

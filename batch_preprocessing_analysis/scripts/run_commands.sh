#!/bin/bash

# Command 1
rm /input/preprocessed/*

# Command 2
spark-submit --master local[*] --driver-memory 6g --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.7 --repositories https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/ /app/batch_preprocessing.py --input_path file:///input/HomeC2.csv

# Command 3
spark-submit --master local[*] /app/batch_analysis.py --input_path file:///input/preprocessed
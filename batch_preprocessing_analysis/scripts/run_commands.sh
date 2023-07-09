#!/bin/bash

# Command 1
#rm /input/preprocessed/*

# Command 2
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.7 --master local[*] --driver-memory 4g /app/batch_preprocessing.py --input_path file:///input/HomeC2.csv

# Command 3
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.7 --master local[*] /app/batch_analysis.py

# Command 4
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,com.github.jnr:jnr-posix:3.1.7 --master local[*] /app/weather_prediction.py
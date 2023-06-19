#!/bin/bash

#Command 1
$HADOOP_HOME/bin/hdfs dfs -put /input/HomeC.csv /input

# Command 2
spark-submit --master yarn[*] /app/batch_preprocessing.py --input_path hdfs:///input/HomeC.csv --output_path hdfs:///output/preprocessed 

# Command 3
saprk-submit --master yarn[*] /app/batch_analysis --input_path hdfs:///output/preprocessed
#!/bin/bash

# Command 1
rm /input/preprocessed/*

# Command 2
spark-submit --master local[*] /app/batch_preprocessing.py --input_path file:///input/HomeC2.csv

# Command 3
spark-submit --master local[*] /app/batch_analysis.py --input_path file:///input/preprocessed
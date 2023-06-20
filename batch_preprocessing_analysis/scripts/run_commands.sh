#!/bin/bash

#ls -l /input

# Command 3
spark-submit --master local[*] /app/batch_preprocessing.py --input_path file:///input/HomeC.csv --output_path file:///input/preprocessed 

# Command 4
spark-submit --master local[*] /app/batch_analysis.py --input_path file:///input/preprocessed
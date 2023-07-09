from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName('PREDICTION') \
    .config('spark.cassandra.connection.host', 'cassandra') \
    .config('spark.cassandra.connection.port', '9042') \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()


df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="weather_prediction", keyspace="my_batch") \
    .load()

#spark = SparkSession.builder.getOrCreate()

#data = spark.read.csv("file:///home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_prediction.csv", header=True, inferSchema=True)

assembler = VectorAssembler(inputCols = df.columns[:-1], outputCol="features")

df = assembler.transform(df).select("features", "temperature")

# Percentuale di dati da utilizzare per l'addestramento
train_ratio = 0.8

# Dividi i dati in set di addestramento e set di test
train_data, test_data = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)

# Crea un'istanza del modello di regressione lineare
lr = LinearRegression(featuresCol="features", labelCol="temperature")

# Addestra il modello sui dati di addestramento
model = lr.fit(train_data)

# Prevedi i valori delle temperature per il set di test
predictions = model.transform(test_data)

# Valuta le prestazioni del modello utilizzando l'evaluatore di regressione
evaluator = RegressionEvaluator(labelCol="temperature", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE):", rmse)

spark.stop()
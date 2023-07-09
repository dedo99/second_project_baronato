from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

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

# spark = SparkSession.builder.getOrCreate()

# df = spark.read.csv("file:///home/pietro/Documenti/BigData/second_project_baronato/datasets/test/weather_prediction.csv", header=True, inferSchema=True)

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

# Evaluation training
trainingSummary = model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# Prevedi i valori delle temperature per il set di test
predictions = model.transform(test_data)
predictions.select("prediction","temperature","features").show(10)
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="temperature",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(predictions))
test_result = model.evaluate(test_data)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

prediction_list = predictions.select('prediction').collect()
prediction_list = [row['prediction'] for row in prediction_list]

true_list = test_data.select('temperature').collect()
true_list = [row['temperature'] for row in true_list]

x_values = [i for i in range(1612)]

fig, ax = plt.subplots()

# Plot the true values line
ax.plot(x_values, true_list, marker='o', linestyle='-', label='True Values')

# Plot the predicted values line
ax.plot(x_values, prediction_list, marker='o', linestyle='--', label='Predicted Values')


# Set labels and title
ax.set_xlabel('Timestamp')
ax.set_ylabel('Temperature')
ax.set_title('Differences between predictions and true values')

# Add legend
ax.legend()

# Show the plot
plt.show()

spark.stop()
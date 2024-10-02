from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
import pandas as pd
spark = SparkSession.builder \
    .appName("K-Means Anomaly Detection on Bitcoin Transactions") \
    .getOrCreate()

delta_output_path = "/tmp/delta/btc_transactions_final"

df = spark.read.format("delta").load(delta_output_path)

df_transformed = df.withColumn(
    "prev_output_value", F.expr("aggregate(prev_out_value, 0L, (acc, x) -> acc + x)")
)
df_transformed = df_transformed.withColumn(
    "output_value", F.expr("aggregate(out_value, 0L, (acc, x) -> acc + x)")
)

df_final = df_transformed.drop("prev_out_value", "out_value")

assembler = VectorAssembler(inputCols=["transaction_fee", "transaction_size", "vin_sz", "vout_sz", "weight", "prev_output_value", "output_value"], outputCol="features")
df_features = assembler.transform(df_final)

# Scaling the features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(df_features)
data_df = scaler_model.transform(df_features)
wssse_values =[]
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='scaled_features', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

for i in range(2,8):    
    KMeans_mod = KMeans(featuresCol='scaled_features', k=i)  
    KMeans_fit = KMeans_mod.fit(data_df)  
    output = KMeans_fit.transform(data_df)   
    score = evaluator.evaluate(output)   
    wssse_values.append(score)  
    print("Silhouette Score:",score)
plt.plot(range(1, 7), wssse_values)
plt.xlabel('Number of Clusters (K)')
plt.ylabel('Within Set Sum of Squared Errors (WSSSE)')
plt.title('Elbow Method for Optimal K')
plt.grid()
plt.show()

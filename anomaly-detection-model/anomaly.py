from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("K-Means Anomaly Detection") \
    .getOrCreate()

# Load your DataFrame from Delta
delta_output_path = "/tmp/delta/btc_transactions_final"
df = spark.read.format("delta").load(delta_output_path)

# Create aggregated columns
df_transformed = df.withColumn(
    "prev_output_value", F.expr("aggregate(prev_out_value, 0L, (acc, x) -> acc + x)")
)
df_transformed = df_transformed.withColumn(
    "output_value", F.expr("aggregate(out_value, 0L, (acc, x) -> acc + x)")
)

# Drop unnecessary columns
df_final = df_transformed.drop("prev_out_value", "out_value")

# Assemble features
assembler = VectorAssembler(inputCols=["transaction_fee", "transaction_size", "vin_sz", "vout_sz", "weight", "prev_output_value", "output_value"], outputCol="features")
df_features = assembler.transform(df_final)

# Train K-Means model
kmeans = KMeans(k=5, seed=1)  # Adjust 'k' based on your data characteristics
model = kmeans.fit(df_features)

# Make predictions
predictions = model.transform(df_features)

# Display the results with predictions
predictions.select("features", "prediction").show(truncate=False)

# Calculate distance to centroid
centers = model.clusterCenters()
centers_broadcast = spark.sparkContext.broadcast(centers)

# UDF to calculate distance to the nearest center
def compute_distance(features, cluster_index):
    center = centers_broadcast.value[cluster_index]
    return float(Vectors.squared_distance(features, center))

compute_distance_udf = F.udf(compute_distance, DoubleType())

# Add distance to centroid for each point
predictions_with_distance = predictions.withColumn("distance_to_centroid", compute_distance_udf("features", "prediction"))

# Find the 5 points with the longest distance to their cluster centers
longest_distances = predictions_with_distance.orderBy(F.desc("distance_to_centroid")).limit(5)

# Show the top 5 points
longest_distances.select("features", "prediction", "distance_to_centroid").show(truncate=False)

# Stop Spark session
spark.stop()

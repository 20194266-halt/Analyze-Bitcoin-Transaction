from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka to Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("_airbyte_ab_id", StringType(), True),
    StructField("_airbyte_stream", StringType(), True),
    StructField("_airbyte_emitted_at", LongType(), True),
    StructField("_airbyte_data", 
        StructType([
            StructField("txs", 
                ArrayType(StructType([
                    StructField("hash", StringType(), True),
                    StructField("fee", IntegerType(), True),
                    StructField("size", IntegerType(), True),
                    StructField("time", IntegerType(), True),
                    StructField("block_height", LongType(), True),
                    StructField("vin_sz", IntegerType(), True),
                    StructField("vout_sz", IntegerType(), True),
                    StructField("weight", IntegerType(), True),
                    StructField("double_spend", BooleanType(), True),
                    StructField("inputs", ArrayType(StructType([
	                    StructField("prev_out", StructType([
	                        StructField("value", LongType(), True),
			            ]), True)
		            ]), True),True),
		            StructField("out", ArrayType(StructType([
			            StructField("value", LongType(), True)
		            ]), True), True)
				]))
            , True)
        ])
    , True)
])

# Read from Kafka stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Test") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka value (assuming it contains JSON in `value` column)
df = kafka_stream.selectExpr("CAST(value AS STRING)")

# Deserialize the JSON into columns
df_parsed = df.select(from_json(col("value"), schema).alias("data"))

# Extract necessary fields
txs_extracted = df_parsed.select(
    explode(col("data._airbyte_data.txs")).alias("txs")
).select(
    col("txs.hash").alias("transaction_hash"),
    col("txs.fee").alias("transaction_fee"),
    col("txs.size").alias("transaction_size"),
    col("txs.time").alias("transaction_time"),
    col("txs.block_height").alias("block_height"),
    col("txs.vin_sz").alias("vin_sz"),
    col("txs.vout_sz").alias("vout_sz"),
    col("txs.weight").alias("weight"),
    col("txs.double_spend").alias("double_spend"),
    col("txs.inputs.prev_out.value").alias("prev_out_value"),
    col("txs.out.value").alias("out_value")
)

# Write to Delta Lake with a checkpoint directory
query = txs_extracted.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/kafka_to_delta") \
    .start("/tmp/delta/btc_transactions_final")

query.awaitTermination()


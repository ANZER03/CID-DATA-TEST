from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time

spark = SparkSession.builder \
    .appName("SparkIcebergMinioTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema of incoming Kafka data
schema = StructType([
    StructField("id_transaction", StringType()),
    StructField("client", StringType()),
    StructField("produit", StringType()),
    StructField("prix", DoubleType()),
    StructField("timestamp", StringType())
])

# Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-spark-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON payload
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Create Nessie Bronze catalog namespace and table in MinIO
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.bronze.transactions (
        id_transaction STRING,
        client STRING,
        produit STRING,
        prix DOUBLE,
        timestamp STRING
    )
    USING iceberg
    LOCATION 's3a://bronze/transactions/'
""")

print("Writing stream to Nessie Iceberg Bronze table in MinIO...")
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "/tmp/spark-checkpoints/bronze/test_transactions") \
    .toTable("nessie.bronze.transactions")

# Run the stream for 20 seconds to process the Kafka message
time.sleep(20)
query.stop()

print("Verifying data written to Nessie Iceberg table:")
spark.sql("SELECT * FROM nessie.bronze.transactions").show(truncate=False)

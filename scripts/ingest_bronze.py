from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id_transaction", StringType()),
    StructField("client", StringType()),
    StructField("produit", StringType()),
    StructField("prix", DoubleType()),
    StructField("timestamp", StringType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_transactions") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

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

query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/tmp/cksh_bronze/transactions") \
    .toTable("nessie.bronze.transactions")

query.awaitTermination()
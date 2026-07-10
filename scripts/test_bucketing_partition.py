from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp, sum, count

# Initialize Spark Session
spark = SparkSession.builder.appName("test_bucketing_partition").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Override Nessie catalog default root to the test bucket for this job
spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://test/")

# ── Read bronze ──────────────────────────────────────────────────────────────
print("Reading from bronze layer...")
bronze_df = spark.table("nessie.bronze.transactions")

# ── Process & Aggregate ──────────────────────────────────────────────────────
# Parse timestamp, aggregate client transactions by hour
aggregated_df = (
    bronze_df
    .withColumn("transaction_time", to_timestamp(col("timestamp")))
    # Normalize timestamp to the start of the hour
    .withColumn("transaction_hour", date_format(col("transaction_time"), "yyyy-MM-dd HH:00:00").cast("timestamp"))
    .groupBy("client", "transaction_hour")
    .agg(
        sum("prix").alias("total_spending"),
        count("id_transaction").alias("transaction_count")
    )
)

# ── Create namespace and table in test catalog ─────────────────────────────────
print("Creating namespace and table in test bucket...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.test")

# Using Iceberg with partitioning and bucketing:
# Partition by hours(transaction_hour) and bucket(4, client)
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.test.transactions_agg (
        client STRING,
        transaction_hour TIMESTAMP,
        total_spending DOUBLE,
        transaction_count LONG
    )
    USING iceberg
    PARTITIONED BY (hours(transaction_hour), bucket(4, client))
    LOCATION 's3a://test/transactions_agg/'
""")

# ── Write to destination table ───────────────────────────────────────────────
print("Writing data to nessie.test.transactions_agg...")
aggregated_df.writeTo("nessie.test.transactions_agg").overwritePartitions()

print("✅ Test Job finished. Aggregated data written successfully to s3a://test/transactions_agg/")
spark.stop()

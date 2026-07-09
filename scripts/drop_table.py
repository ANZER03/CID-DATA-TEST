from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("DROP TABLE IF EXISTS nessie.bronze.transactions")
print("Table nessie.bronze.transactions dropped successfully!")

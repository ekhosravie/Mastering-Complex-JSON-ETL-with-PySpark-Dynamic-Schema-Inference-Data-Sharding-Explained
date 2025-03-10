from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Inspect the file content
print("File head:")
print(dbutils.fs.head("dbfs:/FileStore/sample_data.json", 1000))

# Step 2: Create a Spark session
spark = SparkSession.builder.appName("AutoSchema and Shard JSON").getOrCreate()

# Step 3: Read the JSON file without an explicit schema.
# Using the multiLine option because the JSON is formatted as an array.
df = spark.read.option("multiLine", "true") \
              .json("dbfs:/FileStore/sample_data.json") \
              .cache()

# Force evaluation by counting rows
row_count = df.count()
print("Row count:", row_count)

# Step 4: Print the inferred schema and sample data
print("Inferred Schema of the JSON file:")
df.printSchema()

print("Sample data:")
df.show(5, truncate=False)

# Step 5: Transform the data into two DataFrames

# Create the Profile DataFrame by flattening the nested profile fields.
profile_df = df.select(
    "user_id",
    F.col("profile.name").alias("profile_name"),
    F.col("profile.age").alias("profile_age"),
    F.col("profile.address.street").alias("profile_address_street"),
    F.col("profile.address.city").alias("profile_address_city"),
    F.col("profile.address.zip").alias("profile_address_zip")
)

# Create the Transactions DataFrame by exploding the transactions array.
transactions_df = df.select(
    "user_id",
    F.explode("transactions").alias("transaction")
).select(
    "user_id",
    F.col("transaction.tx_id").alias("tx_id"),
    F.col("transaction.amount").alias("amount"),
    F.col("transaction.date").alias("date")
)

# Print the results to verify
print("Profile DataFrame:")
profile_df.show(truncate=False)

print("Transactions DataFrame:")
transactions_df.show(truncate=False)

# (Optional) Write the DataFrames to Delta Lake
# profile_df.write.format("delta").mode("overwrite").save("dbfs:/mnt/my_mount/delta/profile")
# transactions_df.write.format("delta").mode("overwrite").save("dbfs:/mnt/my_mount/delta/transactions")

# Note: Do not call spark.stop() in Databricks.

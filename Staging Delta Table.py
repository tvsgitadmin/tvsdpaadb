# Databricks notebook source
# Define paths
source_folder = "/mnt/tvsdpa/{config.get("bronze_file_path")}"
delta_table_folder = "/mnt/tvsdpa/{config.get("silver_file_path")}"

# Define table schema and name
schema_name = config.get("staging_schema_name")
table_name = config.get("staging_table_name")

# Ensure the schema exists in the Metastore
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Read CSV files from the source folder
df = (
    spark.read.format("csv")
    .option("header", "true")  # Assuming the CSV files have a header
    .option("inferSchema", "true")  # Infer schema automatically
    .load(source_folder)
)

# Display the DataFrame for verification (optional)
df.display()
print(df.count())

# Write the DataFrame directly to a Delta table within the stg schema
df.write.format("delta").mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")

# Optionally, register the Delta table in the Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS StgSchema.MyDeltaTable
USING DELTA
LOCATION '{delta_table_folder}'
""")

# Verify the Delta table content (optional)
spark.sql(f"SELECT * FROM {schema_name}.{table_name}").show()

df = spark.table(f"{schema_name}.{table_name}")
row_count = df.count()
print(f"Row count: {row_count}")

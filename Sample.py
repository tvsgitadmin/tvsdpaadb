# Databricks notebook source
config_path = "/Workspace/Repos/sairam1985.sai@hotmail.com/tvsdpaadb/config/config.json"


# COMMAND ----------

# Correct path
config_path = "/Workspace/Repos/sairam1985.sai@hotmail.com/tvsdpaadb/config/config.json"

# Read the file using Databricks file system utilities
with open(config_path.replace("/dbfs", "/dbfs"), "r") as file:
    config = json.load(file)

# COMMAND ----------

# DBTITLE 1,Mount ADLS
config_path = "/Workspace/Repos/sairam1985.sai@hotmail.com/tvsdpaadb/config/config.json"

with open(config_path.replace("/dbfs", "/dbfs"), "r") as file:
    config = json.load(file)

storage_account_name = config.get("storage_account_name")
container_name = config.get("container_name")
source_files_path = config.get("source_files_path")

dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point="/mnt/tvsdpa",
    extra_configs={
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get(
            scope="snsttvsdpadevadb", key="scsttvsdpadev"
        )
    },
)

# COMMAND ----------

# List all files in the directory
files = dbutils.fs.ls(f"/mnt/tvsdpa/{source_files_path}")

# Retrieve the reference column count and column names from the configuration
reference_column_count = config.get("source_column_count")
reference_columns = config.get("source_columns")

# Validate and convert the reference column count to an integer
try:
    reference_column_count = int(reference_column_count)
except ValueError:
    raise ValueError("The 'source_column_count' must be an integer. Check the configuration.")

# Split the reference columns into a list
if not reference_columns:
    raise ValueError("The 'source_columns' is missing from the configuration.")
reference_columns_list = [col.strip() for col in reference_columns.split(",")]

print(f"Reference Column Count: {reference_column_count}")
print(f"Reference Columns: {reference_columns_list}")

# Process each file in the directory
for file_info in files:
    file_path = file_info.path

    try:
        # Read the file into a DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Get the column count and column names of the current file
        column_count = len(df.columns)
        file_columns = [col.strip() for col in df.columns]
        print(f"File: {file_path}, Column Count: {column_count}, Columns: {file_columns}")

        # Determine if the file matches the reference column count and names
        if column_count == reference_column_count and set(file_columns) == set(reference_columns_list):
            target_folder = config.get("bronze_file_path")
        else:
            target_folder = config.get("error_file_path")

        if not target_folder:
            raise ValueError("Target folder path is not set in the configuration.")

        # Construct the target path
        target_path = f"/mnt/tvsdpa/{target_folder}/{file_info.name}"

        # Move the file to the appropriate folder
        dbutils.fs.mv(file_path, target_path)
        print(f"Moved File: {file_path} to {target_path}")

    except Exception as e:
        print(f"Error processing file: {file_path}. Error: {e}")


# COMMAND ----------

# DBTITLE 1,Unmount the mount point
dbutils.fs.unmount("/mnt/tvsdpa")

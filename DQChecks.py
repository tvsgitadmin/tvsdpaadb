# Databricks notebook source
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

# Process each file in the directory
for file_info in files:
    file_path = file_info.path

    try:
        # Read the file into a DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Get the column count and column names of the current file
        column_count = len(df.columns)
        file_columns = [col.strip() for col in df.columns]
       
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
        
    except Exception as e:
        print(f"Error processing file: {file_path}. Error: {e}")

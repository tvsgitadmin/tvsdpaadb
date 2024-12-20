# Databricks notebook source
# DBTITLE 1,Mount ADLS
import json

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




# COMMAND ----------

# DBTITLE 1,Unmount the mount point
dbutils.fs.unmount("/mnt/tvsdpa")

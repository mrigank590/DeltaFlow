# Databricks notebook source
# MAGIC %run "./Config"

# COMMAND ----------

"""
Mounts Azure Blob Storage containers to DBFS if they are not already mounted.
This script:
- Retrieves existing mount points once to improve performance.
- Iterates through the list of containers.
- Checks if each container is already mounted.
- Mounts the container if it is not already mounted.
- Logs an appropriate message if the mount point already exists.
"""

existing_mounts = {mount.mountPoint for mount in dbutils.fs.mounts()}

for container in containers:
    mount_point = f"/mnt/{container}"

    if mount_point not in existing_mounts:
        try:

            dbutils.fs.mount(
                source=f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/",
                mount_point=mount_point,
                extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
            )

        except Exception as e:
            print(f"Error mounting {mount_point}: {e}")

    else:
        print(f"Mount point {mount_point} already exists.")

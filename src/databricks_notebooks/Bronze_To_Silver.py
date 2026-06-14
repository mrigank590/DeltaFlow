# Databricks notebook source
# MAGIC %run "./Util"

# COMMAND ----------

"""
Processes all tables in the Bronze layer for each schema and moves them to the Silver layer.
This includes:
- Listing schemas from `schema_names_df`
- Iterating through schemas and fetching table names
- Identifying the latest file in each table's directory
- Reading data from the latest Parquet file
- Processing the data using `process_table_with_profiling`
- Handling errors gracefully
"""

schema_names = [row["SchemaName"] for row in schema_names_df.collect()]

for schema_name in schema_names:
    try:
        items = dbutils.fs.ls(f"dbfs:/mnt/bronze/{schema_name}")
        table_names = [item.name[:-1] for item in items if item.isDir]

        for table in table_names:
            try:
                files = dbutils.fs.ls(f"dbfs:/mnt/bronze/{schema_name}/{table}/{year}/{month}/{day}/")
                if not files:
                    print(f"No files found for table {table}. Skipping...")
                    continue

                latest_file = builtin_max(files, key=lambda file: extract_timestamp(file, table))
                input_path = f"/mnt/bronze/{schema_name}/{table}/{year}/{month}/{day}/{latest_file.name}"
                output_path = f"/mnt/silver/{schema_name}/{table}/{year}/{month}/{day}"
                bad_records_path = f"/mnt/silver/{schema_name}/{table}/BadRecords/{year}/{month}/{day}"

                df = spark.read \
                    .option("mode", "PERMISSIVE") \
                    .option("badRecordsPath", bad_records_path) \
                    .parquet(input_path)

                if df.isEmpty():
                    print(f"No data found for table {table} at {input_path}. Skipping...")
                    continue

                process_table_with_profiling(df, schema_name, table, input_path, output_path, primary_keys, bad_records_path)

            except Exception as e:
                print(f"Error processing {schema_name}.{table}: {e}")

    except Exception as e:
            print(f"Error accessing schema {schema_name}: {e}")

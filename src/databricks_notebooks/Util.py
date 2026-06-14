# Databricks notebook source
# MAGIC %run "./Mount"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    count,
    max,
    min,
    regexp_replace,
    when,
    split,
    expr,
    sum,
    initcap,
    lit,
    current_timestamp,
    to_timestamp,
    approx_count_distinct,
    stddev,
    date_format,
    mean,
    length,
    unix_timestamp
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    FloatType,
    TimestampType,
    DecimalType,
    BinaryType,
    BooleanType,
    NumericType,
    LongType,
    DoubleType
)
import re
from builtins import max as builtin_max

# COMMAND ----------

def extract_timestamp(file_info, table_name):
    """
    Extracts the timestamp from a file name, removes the table name prefix and file extension, 
    and converts it to a datetime object.

    Args:
        file_info (FileInfo): The file information object containing the file name.
        table_name (str): The table name, which serves as the prefix in the file name.

    Returns:
        datetime: The extracted timestamp as a datetime object, or None if an error occurs.
    """
    try:
        file_name = file_info.name
        timestamp_str = file_name.replace(f"{table_name}_", "").replace(".parquet", "")
        return datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
    
    except Exception as e:
        print(f"Error extracting timestamp from {file_info.name}: {e}")
        return None

# COMMAND ----------

def infer_column_type(df, col_name):
    """
    Infers the most suitable data type for a given column by analyzing sample values. 
    The function checks for integer, decimal, timestamp, and string types, 
    updating precision and scale for decimals dynamically.

    It collects sample values from the column, skipping None values, and 
    applies regular expressions to determine if a value is an integer, 
    decimal, or timestamp. The precision and scale are determined for decimal values.

    Args:
        df (DataFrame): The input Spark DataFrame.
        col_name (str): The column name to analyze.

    Returns:
        DataType: The inferred PySpark data type (IntegerType, DecimalType, TimestampType, or StringType).
    """
    try:
        sample_values = df.select(col_name).distinct().limit(100).collect()
        is_int, is_decimal, is_timestamp = True, True, True
        max_precision, max_scale = 18, 2 

        for row in sample_values:
            value = row[col_name]
            if value is None:
                continue 

            value_str = str(value).strip()

            # Check if it's an integer
            if not re.fullmatch(r"-?\d+", value_str):
                is_int = False  

            # Check if it's a decimal (supports precision and scale detection)
            decimal_match = re.fullmatch(r"-?\d+(\.\d+)?", value_str)
            if not decimal_match:
                is_decimal = False
            else:
                # Extract precision and scale dynamically
                if '.' in value_str:
                    precision = len(value_str.replace("-", "").replace(".", ""))
                    scale = len(value_str.split(".")[1])
                else:
                    precision, scale = len(value_str.replace("-", "")), 0

                # Update max precision & scale if necessary
                max_precision = builtin_max(max_precision, precision)
                max_scale = builtin_max(max_scale, scale)

            # Check if it's a timestamp (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?", value_str):
                is_timestamp = False  

        if is_int:
            return IntegerType()
        elif is_decimal:
            return DecimalType(max_precision, max_scale)
        elif is_timestamp:
            return TimestampType()
        else:
            return StringType()
    
    except Exception as e:
        print(f"Error in infer_column_type for column {col_name}: {e}")
        return StringType()

# COMMAND ----------

def type_cast_correct_data_type(df):
    """
    Iterates over the DataFrame's columns and applies inferred data types to ensure correct casting. 
    Only processes columns initially detected as strings.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A new DataFrame with updated column data types.
    """
    try:
        for col_name, data_type in df.dtypes:
            if data_type == "string":
                inferred_type = infer_column_type(df, col_name)

                if isinstance(inferred_type, IntegerType):
                    df = df.withColumn(col_name, col(col_name).cast("int"))
                elif isinstance(inferred_type, DecimalType):
                    df = df.withColumn(col_name, col(col_name).cast(inferred_type))
                elif isinstance(inferred_type, TimestampType):
                    df = df.withColumn(col_name, col(col_name).cast("timestamp"))

        return df  
    
    except Exception as e:
        print(f"Error in type_cast_correct_data_type: {e}")
        return df  

# COMMAND ----------

def rename_columns_with_table_prefix(df, table_name):
    """
    Renames DataFrame columns by adding a table-specific prefix. The table name 
    is processed to extract uppercase letters and convert them to lowercase to 
    form the prefix. This prefix is then added to each column name.

    Args:
        df (DataFrame): The input Spark DataFrame.
        table_name (str): The table name, used for generating the prefix.

    Returns:
        DataFrame: The DataFrame with columns renamed by adding the table-specific prefix.
    """
    try:
        prefix = ''.join(re.findall(r'[A-Z]', table_name)).lower()
        rename_mapping = {col: f"{prefix}_{col}" for col in df.columns}
        
        for old_name, new_name in rename_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        
        return df

    except Exception as e:
        print(f"Error in renaming columns: {e}")
        return df

# COMMAND ----------

def handle_nulls(df):
    """
    Replaces NULL values in DataFrame columns based on their data type. 
    For string columns, NULL values are replaced with "NA". For integer columns, 
    NULL values are replaced with -1. For date and timestamp columns, 
    NULL values are replaced with "1900-01-01". Decimal columns are handled with 
    -1, preserving precision and scale. Unsupported column types are skipped. 

    The function also computes and logs the number of remaining NULL values 
    after the replacement.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: The DataFrame with NULL values replaced and potentially logged warnings.
    """
    try:
        null_counts_before = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

        for col_name, col_type in df.dtypes:
            if col_type == "string":
                default_value = lit("NA")
            elif col_type in ["int", "bigint", "long", "short"]:
                default_value = lit(-1)
            elif col_type in ["timestamp", "date"]:
                default_value = lit("1900-01-01").cast(TimestampType() if col_type == "timestamp" else DateType())
            elif col_type.startswith("decimal"):
                precision, scale = map(int, col_type[8:-1].split(","))
                default_value = lit(-1).cast(DecimalType(precision, scale))
            else:
                continue

            df = df.withColumn(col_name, when(col(col_name).isNull(), default_value).otherwise(col(col_name)))

        null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

        remaining_nulls = {col: count for col, count in null_counts_after.items() if count > 0}
        if remaining_nulls:
            print(f"Warning: Null values still exist after handling: {remaining_nulls}")

        return df
    
    except Exception as e:
        print(f"Error in handling nulls: {e}")
        return df 


# COMMAND ----------

def validate_schema(df_before, df_after):
    """
    Validates schema consistency before and after transformations.

    Args:
        df_before (DataFrame): DataFrame before null handling.
        df_after (DataFrame): DataFrame after null handling.

    Returns:
        None
    """
    schema_before = {field.name: field.dataType.simpleString() for field in df_before.schema.fields}
    schema_after = {field.name: field.dataType.simpleString() for field in df_after.schema.fields}

    if schema_before != schema_after:
        print("Warning: Data type mismatch detected between Source Schema and Destination Schema. Please check your transformations.")

# COMMAND ----------

def upsert_delta_table(delta_table, df, primary_key_columns, current_time, current_user):
    """
    Performs an upsert (merge) into a Delta table by matching records based on primary key columns. 
    If a matching record is found, it updates the existing record with the new data and adds information 
    on who and when it was last updated. If no match is found, a new record is inserted with the 
    current user and timestamp as metadata.

    The merge operation updates the following fields for both matched and new records:
        - `last_updated_by`: The user performing the operation.
        - `last_updated_date`: The timestamp when the update occurs.
        - For new records: `created_by` and `created_date`.

    Args:
        delta_table (DeltaTable): The target Delta table to perform the upsert into.
        df (DataFrame): DataFrame containing the new data to be merged.
        primary_key_columns (list): List of primary key column names to match records.
        current_time (Timestamp): Current timestamp to set for `last_updated_date` and `created_date`.
        current_user (str): Username of the user performing the operation.

    Returns:
        None
    """
    try:
        if not primary_key_columns:
            raise ValueError("Primary key columns must be provided.")

        merge_condition = " AND ".join([f"existing.{key} = updates.{key}" for key in primary_key_columns])

        delta_table.alias("existing") \
            .merge(df.alias("updates"), merge_condition) \
            .whenMatchedUpdate(set={
                "last_updated_by": lit(current_user),
                "last_updated_date": current_time,
                **{col: expr(f"updates.{col}") for col in df.columns}
            }) \
            .whenNotMatchedInsert(values={
                "created_by": lit(current_user),
                "created_date": current_time,
                "last_updated_by": lit(current_user),
                "last_updated_date": current_time,
                **{col: expr(f"updates.{col}") for col in df.columns}
            }) \
            .execute()

    except Exception as e:
        print(f"Error in upsert function: {e}")


# COMMAND ----------

def compute_profiling_stats(df, bad_records_path, schema_name, table_name):
    """
    Computes profiling statistics for a given DataFrame, such as min, max, distinct count, 
    null count, standard deviation, mean, and sum, and writes the results to Delta Lake.

    The profiling is performed based on the column data types. For numeric types, 
    statistics like min, max, mean, and sum are computed. For date and timestamp types, 
    min and max values along with distinct count and null count are computed. 
    For other types, only min, max, distinct count, and null count are calculated.

    The results are written to a Delta table in the specified path, and any bad records are stored 
    in the provided bad records path.

    Args:
        df (DataFrame): Input DataFrame to profile.
        bad_records_path (str): Path for storing bad records that could not be processed.
        schema_name (str): The schema name of the table.
        table_name (str): The table name being profiled.

    Returns:
        None
    """
    try:
        original_schema = {field.name: field.dataType for field in df.schema.fields}
        original_columns = df.columns

        profiling_stats = []
        for col_name in original_columns:
            actual_data_type = original_schema[col_name] 

            col_min, col_max, col_distinct_count, col_null_count, col_std_dev, col_mean, col_sum = (None,)*7

            if isinstance(actual_data_type, (NumericType, DoubleType, DecimalType, LongType)):
                stats = df.agg(
                    min(col(col_name).cast("double")).alias("min_value"),
                    max(col(col_name).cast("double")).alias("max_value"),
                    approx_count_distinct(col(col_name).cast("double")).alias("distinct_count"),
                    count(when(col(col_name).isNull(), col_name)).alias("null_count"),
                    stddev(col(col_name).cast("double")).alias("std_dev"),
                    mean(col(col_name).cast("double")).alias("mean_value"),
                    sum(col(col_name).cast("double")).alias("sum_value")
                ).collect()[0]

                col_min, col_max, col_distinct_count, col_null_count, col_std_dev, col_mean, col_sum = stats

            elif isinstance(actual_data_type, (DateType, TimestampType)):
                stats = df.agg(
                    min(col(col_name).cast("date")).alias("min_value"),
                    max(col(col_name).cast("date")).alias("max_value"),
                    approx_count_distinct(col(col_name).cast("date")).alias("distinct_count"),
                    count(when(col(col_name).isNull(), col_name)).alias("null_count")
                ).collect()[0]

                col_min, col_max, col_distinct_count, col_null_count = stats

            else:
                stats = df.agg(
                    min(col(col_name)).alias("min_value"),
                    max(col(col_name)).alias("max_value"),
                    approx_count_distinct(col(col_name)).alias("distinct_count"),
                    count(when(col(col_name).isNull(), col_name)).alias("null_count")
                ).collect()[0]

                col_min, col_max, col_distinct_count, col_null_count = stats

            profiling_stats.append({
                "column_name": str(col_name),
                "min_value": str(col_min),
                "max_value": str(col_max),
                "distinct_count": str(col_distinct_count),
                "null_count": str(col_null_count),
                "std_dev": str(col_std_dev) if col_std_dev is not None else None,
                "mean_value": str(col_mean) if col_mean is not None else None,
                "sum_value": str(col_sum) if col_sum is not None else None
            })

        profiling_df = spark.createDataFrame(profiling_stats)

        profiling_path = f"/mnt/silver/{schema_name}/ProfileStats/{table_name}/{year}/{month}/{day}"

        profiling_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("badRecordsPath", bad_records_path) \
            .save(profiling_path)

    except Exception as e:
        raise RuntimeError(f"Error in getting profiling stats: {e}")


# COMMAND ----------

def process_table_with_profiling(df, schema_name, table_name, input_path, output_path, primary_keys, bad_records_path):
    """
    Processes a table from the Bronze layer to the Silver layer, performing several operations including:
    - Type casting data to the correct data types (for "FileIngest" schema).
    - Renaming columns by adding a table-specific prefix.
    - Handling NULL values by replacing them with defaults based on column types.
    - Validating schema consistency after renaming and handling NULLs.
    - Performing an upsert (merge) operation into a Delta table.
    - Saving profiling statistics to Delta Lake.

    This function handles errors gracefully and logs any issues encountered during the process.

    Args:
        df (DataFrame): Input DataFrame that needs to be processed.
        schema_name (str): Schema name of the table (e.g., "FileIngest").
        table_name (str): Name of the table being processed.
        input_path (str): Path where the input data is stored.
        output_path (str): Path where the output Delta table will be saved.
        primary_keys (dict): Dictionary containing the primary key columns for each table.
        bad_records_path (str): Path for storing bad records that couldn't be processed.

    Returns:
        None
    """
    try:
        if schema_name == "FileIngest":
            df = type_cast_correct_data_type(df)
        
        df_renamed = rename_columns_with_table_prefix(df, table_name)
        
        df_no_nulls = handle_nulls(df_renamed)

        validate_schema(df_renamed, df_no_nulls)

        current_time = to_timestamp(lit(curr_time), "yyyy-MM-dd_HH-mm-ss")
        primary_key_columns = primary_keys.get(table_name)

        if not primary_key_columns:
            raise ValueError(f"Primary key not defined for table: {table_name}")

        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        full_table_name = f"silver.{table_name}"

        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            upsert_delta_table(delta_table, df_no_nulls, primary_key_columns, current_time, current_user)

            delta_table = DeltaTable.forName(spark, full_table_name)
            upsert_delta_table(delta_table, df_no_nulls, primary_key_columns, current_time, current_user)
        else:
            df_audited = df_no_nulls \
                .withColumn("created_by", lit(current_user)) \
                .withColumn("created_date", current_time) \
                .withColumn("last_updated_by", lit(current_user)) \
                .withColumn("last_updated_date", current_time)

            df_audited.write.format("delta").mode("overwrite") \
                .option("badRecordsPath", bad_records_path) \
                .save(output_path)

            df_audited.write.format("delta").mode("overwrite") \
                .option("badRecordsPath", bad_records_path) \
                .saveAsTable(full_table_name)

        compute_profiling_stats(df_no_nulls, bad_records_path, schema_name, table_name)

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")

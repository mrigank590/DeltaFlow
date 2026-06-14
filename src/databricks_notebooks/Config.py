# Databricks notebook source
containers=["bronze","silver","gold"]
storage_account_name="mnsteus"
storage_account_key=dbutils.secrets.get(scope="mn-kv-eus", key="ADLS-connection")
sql_pass=dbutils.secrets.get(scope="mn-kv-eus", key="SQL-password")

# COMMAND ----------

primary_keys = {
    "Customer": ["c_CustomerID"],
    "ProductModel": ["pm_ProductModelID"],
    "ProductDescription": ["pd_ProductDescriptionID"],
    "Product": ["p_ProductID"],
    "ProductModelProductDescription": ["pmpd_ProductModelID", "pmpd_ProductDescriptionID", "pmpd_Culture"],
    "ProductCategory": ["pc_ProductCategoryID"],
    "Address": ["a_AddressID"],
    "CustomerAddress": ["ca_CustomerID", "ca_AddressID"],
    "SalesOrderDetail": ["sod_SalesOrderID", "sod_SalesOrderDetailID"],
    "SalesOrderHeader": ["soh_SalesOrderID"],
    "CustomerSourceDemo": ["csd_CustomerID"]
}

# COMMAND ----------

from datetime import datetime
current_date = datetime.now()
year = current_date.strftime("%Y")
month = current_date.strftime("%m") 
day = current_date.strftime("%d")
curr_time = dbutils.widgets.get("Current_Timestamp")
source_type = dbutils.widgets.get("Source_Type")

# COMMAND ----------

sql_server = dbutils.secrets.get(scope="mn-kv-eus", key="SQL_SERVER")
database = dbutils.secrets.get(scope="mn-kv-eus", key="SQL_DATABASE")
user = dbutils.secrets.get(scope="mn-kv-eus", key="SQL_USER")

schema_names_df = (spark.read
  .format("jdbc")
  .option("url", f"jdbc:sqlserver://{sql_server};database={database};user={user};password={sql_pass};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("query", f"SELECT DISTINCT SchemaName FROM Metastore.Table_Details WHERE SourceType = '{source_type}'")
  .load()
)

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

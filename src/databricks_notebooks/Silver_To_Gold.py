# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
gold_path="/mnt/gold/SalesLT/"

# COMMAND ----------

# spark.sql("drop schema gold cascade")

# COMMAND ----------

dim_customer_details=spark.sql("""
SELECT
    c.*,
    ca.ca_AddressType,
    a.a_AddressLine1,
    a.a_AddressLine2,
    a.a_City,
    a.a_StateProvince,
    a.a_CountryRegion,
    a.a_PostalCode
FROM 
    silver.customer c
LEFT JOIN 
    silver.customeraddress ca ON c.c_CustomerID = ca.ca_CustomerID
LEFT JOIN 
    silver.address a ON ca.ca_AddressID = a.a_AddressID;
""")

# COMMAND ----------

dim_customer_details.write.mode("overwrite").format("delta").option("path",gold_path+"dim_customer_details").saveAsTable("gold.dim_customer_details")

# COMMAND ----------

dim_product_details=spark.sql("""
SELECT
    p.*,
    pc.pc_Name,
    pd.pd_Description,
    pm.pm_Name,
    pm.pm_CatalogDescription,
    pmpd.pmpd_Culture
FROM 
    silver.product p
LEFT JOIN 
    silver.productcategory pc ON p.p_ProductCategoryID = pc.pc_ProductCategoryID
LEFT JOIN 
    silver.productmodelproductdescription pmpd ON p.p_ProductModelID = pmpd.pmpd_ProductModelID
LEFT JOIN 
    silver.productdescription pd ON pmpd.pmpd_ProductDescriptionID = pd.pd_ProductDescriptionID
LEFT JOIN 
    silver.productmodel pm ON p.p_ProductModelID = pm.pm_ProductModelID;
""")

# COMMAND ----------

dim_product_details.write.mode("overwrite").format("delta").option("path",gold_path+"dim_product_details").saveAsTable("gold.dim_product_details")

# COMMAND ----------

dim_sales_details=spark.sql("""
SELECT
    soh.*,
    sod.sod_SalesOrderDetailID,
    sod.sod_OrderQty,
    sod.sod_ProductID,
    sod.sod_UnitPrice,
    sod.sod_UnitPriceDiscount,
    sod.sod_LineTotal
FROM 
    silver.salesorderheader soh
LEFT JOIN 
    silver.salesorderdetail sod ON soh.soh_SalesOrderID = sod.sod_SalesOrderID;
""")

# COMMAND ----------

dim_sales_details.write.mode("overwrite").format("delta").option("path",gold_path+"dim_sales_details").saveAsTable("gold.dim_sales_details")

# COMMAND ----------

dim_fact_table = spark.sql("""
SELECT
    -- Customer Columns
    c.c_CustomerID AS CustomerID,
    c.c_NameStyle AS NameStyle,
    c.c_Title AS Title,
    c.c_FirstName AS FirstName,
    c.c_MiddleName AS MiddleName,
    c.c_LastName AS LastName,
    c.c_Suffix AS Suffix,
    c.c_CompanyName AS CompanyName,
    c.c_SalesPerson AS SalesPerson,
    c.c_EmailAddress AS EmailAddress,
    c.c_Phone AS Phone,
    c.c_PasswordHash AS PasswordHash,
    c.c_PasswordSalt AS PasswordSalt,
    c.a_AddressLine1 AS AddressLine1,
    c.a_City AS City,
    c.a_StateProvince AS StateProvince,
    c.a_CountryRegion AS CountryRegion,
    
    -- Product Columns
    p.p_ProductID AS ProductID,
    p.p_Name AS ProductName,
    p.p_ProductNumber AS ProductNumber,
    p.p_Color AS Color,
    p.p_StandardCost AS StandardCost,
    p.p_ListPrice AS ListPrice,
    p.p_Size AS Size,
    p.p_Weight AS Weight,
    p.p_ProductCategoryID AS ProductCategoryID,
    p.p_ProductModelID AS ProductModelID,
    p.p_SellStartDate AS SellStartDate,
    p.p_SellEndDate AS SellEndDate,
    p.p_DiscontinuedDate AS DiscontinuedDate,
    p.pc_Name AS Category,
    p.pd_Description AS Description,
    p.pm_Name AS Model,
    p.pm_CatalogDescription AS CatalogDescription,
    p.pmpd_Culture AS Culture,

    -- Sales Columns
    s.soh_SalesOrderID AS SalesOrderID,
    s.soh_RevisionNumber AS RevisionNumber,
    s.soh_OrderDate AS OrderDate,
    s.soh_DueDate AS DueDate,
    s.soh_ShipDate AS ShipDate,
    s.soh_Status AS Status,
    s.soh_OnlineOrderFlag AS OnlineOrderFlag,
    s.soh_SalesOrderNumber AS SalesOrderNumber,
    s.soh_PurchaseOrderNumber AS PurchaseOrderNumber,
    s.soh_AccountNumber AS AccountNumber,
    s.soh_CustomerID AS CustomerID_Sales,
    s.soh_ShipToAddressID AS ShipToAddressID,
    s.soh_BillToAddressID AS BillToAddressID,
    s.soh_ShipMethod AS ShipMethod,
    s.soh_CreditCardApprovalCode AS CreditCardApprovalCode,
    s.soh_SubTotal AS SubTotal,
    s.soh_TaxAmt AS TaxAmount,
    s.soh_Freight AS Freight,
    s.soh_TotalDue AS TotalDue,
    s.soh_Comment AS Comment,
    s.soh_rowguid AS RowGUID,
    s.soh_ModifiedDate AS ModifiedDate,
    s.sod_SalesOrderDetailID AS SalesOrderDetailID,
    s.sod_OrderQty AS OrderQuantity,
    s.sod_ProductID AS ProductID_Sales,
    s.sod_UnitPrice AS UnitPrice,
    s.sod_UnitPriceDiscount AS UnitPriceDiscount,
    s.sod_LineTotal AS LineTotal,
    s.soh_TotalDue AS SalesAmount

FROM 
    gold.dim_customer_details c
JOIN 
    gold.dim_sales_details s ON c.c_CustomerID = s.soh_CustomerID
JOIN 
    gold.dim_product_details p ON s.sod_ProductID = p.p_ProductID;
""")


# COMMAND ----------

dim_fact_table.write.mode("overwrite").format("delta").option("path",gold_path+"dim_fact_table").saveAsTable("gold.dim_fact_table")

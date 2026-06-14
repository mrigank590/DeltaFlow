CREATE TABLE [Metastore].[Table_Details] (
    DBName NVARCHAR(100),
    SchemaName NVARCHAR(100),
    TableName NVARCHAR(100),
    FileName NVARCHAR(100),
    SourceType NVARCHAR(100),
    PrimaryKeyColumns NVARCHAR(MAX),              -- Comma-separated list of PK columns
    WatermarkColumn NVARCHAR(100),
    WatermarkValue NVARCHAR(50)
);

INSERT INTO [Metastore].[Table_Details] 
(DBName, TableName, SchemaName, SourceType, WatermarkColumn, PrimaryKeyColumns)
VALUES
('mn-sql-db-eus-001', 'Address', 'SalesLT', 'SQL', 'ModifiedDate', 'AddressID'),
('mn-sql-db-eus-001', 'Customer', 'SalesLT', 'SQL', 'ModifiedDate', 'CustomerID'),
('mn-sql-db-eus-001', 'CustomerAddress', 'SalesLT', 'SQL', 'ModifiedDate', 'CustomerID,AddressID'),
('mn-sql-db-eus-001', 'Product', 'SalesLT', 'SQL', 'ModifiedDate', 'ProductID'),
('mn-sql-db-eus-001', 'ProductCategory', 'SalesLT', 'SQL', 'ModifiedDate', 'ProductCategoryID'),
('mn-sql-db-eus-001', 'ProductDescription', 'SalesLT', 'SQL', 'ModifiedDate', 'ProductDescriptionID'),
('mn-sql-db-eus-001', 'ProductModel', 'SalesLT', 'SQL', 'ModifiedDate', 'ProductModelID'),
('mn-sql-db-eus-001', 'ProductModelProductDescription', 'SalesLT', 'SQL', 'ModifiedDate', 'ProductModelID,ProductDescriptionID,Culture'),
('mn-sql-db-eus-001', 'SalesOrderDetail', 'SalesLT', 'SQL', 'ModifiedDate', 'SalesOrderID,SalesOrderDetailID'),
('mn-sql-db-eus-001', 'SalesOrderHeader', 'SalesLT', 'SQL', 'ModifiedDate', 'SalesOrderID');

INSERT INTO Metastore.Table_Details 
(FileName, SchemaName, SourceType, WatermarkColumn, PrimaryKeyColumns)
VALUES
('CustomerSourceDemo.csv', 'FileIngest', 'File', 'Timestamp', 'CustomerID')

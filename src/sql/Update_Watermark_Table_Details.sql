CREATE OR ALTER PROCEDURE Metastore.UpdateWatermarkTableDetails
    @WatermarkValue NVARCHAR(255),
    @TableName NVARCHAR(255),
    @SchemaName NVARCHAR(255)
AS
BEGIN
    UPDATE Metastore.Table_Details
    SET 
        WatermarkValue = @WatermarkValue
    WHERE 
        TableName = @TableName
        AND SchemaName = @SchemaName;
END;

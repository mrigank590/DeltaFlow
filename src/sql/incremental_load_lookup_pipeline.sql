SELECT  
    td.DBName,  
    td.TableName,  
    td.SchemaName,  
    td.FileName,  
    td.SourceType,  
    td.WatermarkColumn,  
    td.WatermarkValue 
FROM Metastore.Table_Details td  
LEFT JOIN (  
    SELECT  
        table_name,  
        file_name,  
        MAX(job_end_date) AS max_job_end_date  
    FROM Metastore.Pipeline_Details  
    WHERE table_load_status = 'Succeeded'
    GROUP BY table_name, file_name  
) pd  
ON ISNULL(td.TableName, '') = ISNULL(pd.Table_name, '') 
AND ISNULL(td.FileName, '') = ISNULL(pd.file_name, '')  -- Handling NULL file names  
WHERE  
    (
        pd.max_job_end_date IS NULL  -- Table was never loaded  
        OR  
        pd.max_job_end_date < DATEADD(HOUR, -24, GETDATE())  -- Last load was over 24 hours ago  
    )
    AND td.SourceType = '@{pipeline().parameters.source_name}';

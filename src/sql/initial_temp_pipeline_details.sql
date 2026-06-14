CREATE OR ALTER PROCEDURE Metastore.InsertInitialPipelineDetails
    @ADFName NVARCHAR(255),
    @pipeline_name NVARCHAR(255),
    @pipeline_group_id NVARCHAR(255),
    @pipeline_run_id NVARCHAR(255),
    @pipeline_trigger_type NVARCHAR(255),
    @job_start_date DATETIME,
    @job_end_date DATETIME,
    @source_type NVARCHAR(255),
    @db_name NVARCHAR(255),
    @schema_name NVARCHAR(255),
    @table_name NVARCHAR(255),
    @file_name NVARCHAR(255),
    @WatermarkValue NVARCHAR(255),
    @data_read BIGINT,
    @data_written BIGINT,
    @files_read BIGINT,
    @files_written BIGINT,
    @table_load_status NVARCHAR(255)
AS
BEGIN
    -- Declare additional variables
    DECLARE @acquisition_strategy NVARCHAR(255), 
            @merge_strategy NVARCHAR(255),
            @file_count INT,
            @job_duration INT;
    
    select @job_duration= DATEDIFF(SECOND, @job_start_date, @job_end_date)

    -- Check the count of file_name in Metastore.Pipeline_Details
    SELECT @file_count = COUNT(*) 
    FROM Metastore.Pipeline_Details 
    WHERE file_name = @file_name;

    -- Determine acquisition and merge strategies based on file_name and watermark value
    IF @file_name IS NULL OR @file_name = ''
    BEGIN
        IF @WatermarkValue IS NULL OR @WatermarkValue = ''
        BEGIN
            SET @acquisition_strategy = 'Full Load';
            SET @merge_strategy = 'Truncate & Load';
        END
        ELSE
        BEGIN
            SET @acquisition_strategy = 'Incremental Load';
            SET @merge_strategy = 'PK-Based Upsert';
        END;
    END
    ELSE
    BEGIN
        IF @file_count = 0  -- No previous occurrences of the file
        BEGIN
            SET @acquisition_strategy = 'Full Load';
            SET @merge_strategy = 'Truncate & Load';
        END
        ELSE  -- File already exists in the table
        BEGIN
            SET @acquisition_strategy = 'Incremental Load';
            SET @merge_strategy = 'PK-Based Upsert';
        END;
    END

    -- Insert data into Temp_Pipeline_Details table
    INSERT INTO Temp_Pipeline_Details (
        ADFName,
        pipeline_name,
        pipeline_group_id,
        pipeline_run_id,
        pipeline_trigger_type,
        job_start_date,
        job_end_date,
        source_type,
        db_name,
        schema_name,
        table_name,
        file_name,
        acquisition_strategy,
        merge_strategy,
        data_read,
        data_written,
        files_read,
        files_written,
        table_load_status,
        job_duration
    )
    VALUES (
        @ADFName,
        @pipeline_name,
        @pipeline_group_id,
        @pipeline_run_id,
        @pipeline_trigger_type,
        @job_start_date,
        @job_end_date,
        @source_type,
        @db_name,
        @schema_name,
        @table_name,
        @file_name,
        @acquisition_strategy,
        @merge_strategy,
        @data_read,
        @data_written,
        @files_read,
        @files_written,
        @table_load_status,
        @job_duration
    );
END;

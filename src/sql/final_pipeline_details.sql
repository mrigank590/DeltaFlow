CREATE OR ALTER PROCEDURE Metastore.MasterPipelineDetails
    @ADFName NVARCHAR(255),
    @pipeline_name NVARCHAR(255),
    @pipeline_group_id NVARCHAR(255),
    @pipeline_run_id NVARCHAR(255),
    @pipeline_trigger_type NVARCHAR(255),
    @job_start_date DATETIME,
    @job_end_date DATETIME,
    @pipeline_status NVARCHAR(255)
    
AS
BEGIN
    Declare @job_duration INT
    select @job_duration= DATEDIFF(SECOND, @job_start_date, @job_end_date)

    UPDATE Metastore.Temp_Pipeline_Details
    SET 
        pipeline_status = @pipeline_status
    WHERE pipeline_status IS NULL;

    INSERT INTO Metastore.Pipeline_Details
    SELECT *
    FROM Metastore.Temp_Pipeline_Details;

    TRUNCATE TABLE Metastore.Temp_Pipeline_Details;

    INSERT INTO Metastore.Pipeline_Details (
        ADFName,
        pipeline_name,
        pipeline_group_id,
        pipeline_run_id,
        pipeline_trigger_type,
        job_start_date,
        job_end_date,
        job_duration,
        pipeline_status
    )
    VALUES (
        @ADFName,
        @pipeline_name,
        @pipeline_group_id,
        @pipeline_run_id,
        @pipeline_trigger_type,
        @job_start_date,
        @job_end_date,
        @job_duration,
        @pipeline_status
    );
END;

CREATE OR ALTER PROCEDURE sp_CalculateFormulas
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE t_results;

    DECLARE @targil_id INT;
    DECLARE @formula NVARCHAR(MAX);
    DECLARE @tnai NVARCHAR(MAX);
    DECLARE @targil_false NVARCHAR(MAX);
    DECLARE @dynamicSQL NVARCHAR(MAX);
    DECLARE @startTime DATETIME;
    DECLARE @endTime DATETIME;
    DECLARE @runTime FLOAT;

    DECLARE targil_cursor CURSOR FOR 
    SELECT targil_id, targil, tnai, targil_false FROM t_targil;

    OPEN targil_cursor;
    FETCH NEXT FROM targil_cursor INTO @targil_id, @formula, @tnai, @targil_false;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fullExpression NVARCHAR(MAX);
        
        IF @tnai IS NOT NULL AND LTRIM(RTRIM(@tnai)) <> ''
            SET @fullExpression = 'CASE WHEN ' + @tnai + ' THEN ' + @formula + ' ELSE ' + @targil_false + ' END';
        ELSE
            SET @fullExpression = @formula;

        SET @dynamicSQL = N'INSERT INTO t_results (data_id, targil_id, method, result) ' +
                         N'SELECT data_id, ' + CAST(@targil_id AS VARCHAR) + 
                         N', ''Stored Procedure'', ' + @fullExpression + 
                         N' FROM t_data';

        SET @startTime = GETDATE();

        EXEC sp_executesql @dynamicSQL;

        SET @endTime = GETDATE();
        
        SET @runTime = DATEDIFF(ms, @startTime, @endTime) / 1000.0;

        INSERT INTO t_log (targil_id, method, run_time)
        VALUES (@targil_id, 'Stored Procedure', @runTime);

        PRINT 'Formula ' + CAST(@targil_id AS VARCHAR) + ' completed in ' + CAST(@runTime AS VARCHAR) + ' seconds.';

        FETCH NEXT FROM targil_cursor INTO @targil_id, @formula, @tnai, @targil_false;
    END

    CLOSE targil_cursor;
    DEALLOCATE targil_cursor;
END
GO

EXEC sp_CalculateFormulas;
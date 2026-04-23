CREATE OR ALTER PROCEDURE sp_CalculateFormulas
AS
BEGIN
    SET NOCOUNT ON;

	-- Clear previous results
    TRUNCATE TABLE t_results;

	-- Variables for current formula
    DECLARE @targil_id INT;
    DECLARE @formula NVARCHAR(MAX);
    DECLARE @tnai NVARCHAR(MAX);
    DECLARE @targil_false NVARCHAR(MAX);

	-- Dynamic SQL + timing
    DECLARE @dynamicSQL NVARCHAR(MAX);
    DECLARE @startTime DATETIME;
    DECLARE @endTime DATETIME;
    DECLARE @runTime FLOAT;
		
	-- Cursor to iterate over formulas
    DECLARE targil_cursor CURSOR FOR 
    SELECT targil_id, targil, tnai, targil_false FROM t_targil;

    OPEN targil_cursor;
    FETCH NEXT FROM targil_cursor INTO @targil_id, @formula, @tnai, @targil_false;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fullExpression NVARCHAR(MAX);
        
		-- Build expression (with optional condition)
        IF @tnai IS NOT NULL AND LTRIM(RTRIM(@tnai)) <> ''
            SET @fullExpression = 'CASE WHEN ' + @tnai + ' THEN ' + @formula + ' ELSE ' + @targil_false + ' END';
        ELSE
            SET @fullExpression = @formula;

		-- Build dynamic SQL: calculate for all rows at once
        SET @dynamicSQL = N'INSERT INTO t_results (data_id, targil_id, method, result) ' +
                         N'SELECT data_id, ' + CAST(@targil_id AS VARCHAR) + 
                         N', ''Stored Procedure'', ' + @fullExpression + 
                         N' FROM t_data';
		-- Start timing
        SET @startTime = GETDATE();

		-- Execute dynamic calculation
        EXEC sp_executesql @dynamicSQL;

		-- End timing
        SET @endTime = GETDATE();
        
		-- Calculate runtime (seconds)
        SET @runTime = DATEDIFF(ms, @startTime, @endTime) / 1000.0;

		-- Log execution
        INSERT INTO t_log (targil_id, method, run_time)
        VALUES (@targil_id, 'Stored Procedure', @runTime);

		-- Next formula
        PRINT 'Formula ' + CAST(@targil_id AS VARCHAR) + ' completed in ' + CAST(@runTime AS VARCHAR) + ' seconds.';

        FETCH NEXT FROM targil_cursor INTO @targil_id, @formula, @tnai, @targil_false;
    END

	-- Cleanup
    CLOSE targil_cursor;
    DEALLOCATE targil_cursor;
END
GO

EXEC sp_CalculateFormulas;
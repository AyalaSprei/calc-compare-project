WITH RawData AS (
    SELECT 
        l.targil_id, 
        MAX(CASE WHEN l.method = 'Stored Procedure' THEN l.run_time END) AS sqlTime,
        MAX(CASE WHEN l.method = 'C#' THEN l.run_time END) AS csharpTime,
        MAX(CASE WHEN l.method = 'Python' THEN l.run_time END) AS pythonTime
    FROM t_log l
    JOIN t_targil t ON l.targil_id = t.targil_id
    GROUP BY l.targil_id
),
Averages AS (
    SELECT 
        AVG(sqlTime) as avgSql,
        AVG(csharpTime) as avgCsharp,
        AVG(pythonTime) as avgPython
    FROM RawData
),
Mismatches AS (
    -- חישוב כמות השורות שיש בהן חוסר התאמה בין המנועים
    SELECT COUNT(*) AS totalMismatched
    FROM t_results p
    JOIN t_results s ON p.data_id = s.data_id AND p.targil_id = s.targil_id AND s.method = 'SQL'
    JOIN t_results c ON p.data_id = c.data_id AND p.targil_id = c.targil_id AND c.method = 'C#'
    WHERE p.method = 'Python'
      AND (p.result <> s.result OR p.result <> c.result)
)
SELECT 
    (SELECT totalMismatched FROM Mismatches) AS mismatchedCount,
    (SELECT avgSql, avgCsharp, avgPython FROM Averages FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS overallAverages,
    (SELECT * FROM RawData FOR JSON PATH) AS tableDetails
FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
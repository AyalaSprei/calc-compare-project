SELECT 
    p.data_id,
    p.targil_id,
    p.result AS result_python,
    s.result AS result_sql,
    c.result AS result_csharp
FROM t_results p

JOIN t_results s ON p.data_id = s.data_id 
                AND p.targil_id = s.targil_id 
                AND s.method = 'SQL'

JOIN t_results c ON p.data_id = c.data_id 
                AND p.targil_id = c.targil_id 
                AND c.method = 'C#'

WHERE p.method = 'Python'
  AND (
      p.result <> s.result --SQL האם תוצאות פייתון שונות מתוצאות  
      OR 
      p.result <> c.result  -- C#האם תוצאות פייתון שונות מתוצאות    
  );
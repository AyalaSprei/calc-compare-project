WITH 
    L0 AS (SELECT 1 AS c UNION ALL SELECT 1), 
    L1 AS (SELECT 1 AS c FROM L0 AS A CROSS JOIN L0 AS B),
    L2 AS (SELECT 1 AS c FROM L1 AS A CROSS JOIN L1 AS B), 
    L3 AS (SELECT 1 AS c FROM L2 AS A CROSS JOIN L2 AS B), 
    L4 AS (SELECT 1 AS c FROM L3 AS A CROSS JOIN L3 AS B), 
    L5 AS (SELECT 1 AS c FROM L4 AS A CROSS JOIN L4 AS B),
    Nums AS (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS n FROM L5)
INSERT INTO t_data (a, b, c, d)
SELECT TOP 1000000
    ABS(CHECKSUM(NEWID()) % 100) + RAND(), 
    ABS(CHECKSUM(NEWID()) % 100) + RAND(), 
    ABS(CHECKSUM(NEWID()) % 100) + RAND(), 
    ABS(CHECKSUM(NEWID()) % 100) + RAND() 
FROM Nums;

SELECT COUNT(*) AS TotalRows FROM t_data;

INSERT INTO t_targil (targil, tnai, targil_false)
VALUES 
('a + b - (c * 0.5)', NULL, NULL),

('POWER(a, 2) + SQRT(ABS(b))', NULL, NULL),

('(a * d) / (b + c + 1)', NULL, NULL),

('a * 2.5', 'a > b', '(c + d) / 2'),

('(a + b) * 1.15', '(a + b) > (c + d)', 'a - d');

SELECT * FROM t_targil;


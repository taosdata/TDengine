-- Demo SQL queries for performance testing
-- These queries will be used by JMeter for load testing

-- Basic count query
SELECT COUNT(*) FROM test.stb;

-- Last value query
SELECT LAST(*) FROM test.stb;

-- Aggregation query
SELECT AVG(c1), MAX(c1), MIN(c1) FROM test.stb;

-- Time range query
SELECT * FROM test.stb WHERE ts >= NOW - 1h LIMIT 100;

-- Group by query
SELECT COUNT(*), AVG(c1) FROM test.stb GROUP BY tbname LIMIT 10;
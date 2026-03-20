-- BLOB Functions Test Suite
-- Converted from test_fun_sca_blob.py

-- ============================================
-- Test 1: BLOB Basic Operations
-- ============================================
DROP DATABASE IF EXISTS db_blob_basic;
CREATE DATABASE db_blob_basic;
USE db_blob_basic;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));

INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine is great', 'test2');
INSERT INTO test_blob VALUES (NOW, 3, '1234567890', 'test3');
INSERT INTO test_blob VALUES (NOW, 4, 'binary test', 'test4');
INSERT INTO test_blob VALUES (NOW, 5, '中文测试', 'test5');

SELECT 'Test 1: BLOB Basic Operations - length function' as test_case;
SELECT id, length(b_data) as blob_len FROM test_blob WHERE id = 1;

-- ============================================
-- Test 2: VARCHAR to BLOB Cast
-- ============================================
DROP DATABASE IF EXISTS db_blob_cast_v2b;
CREATE DATABASE db_blob_cast_v2b;
USE db_blob_cast_v2b;

CREATE TABLE test_cast (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));

SELECT 'Test 2: VARCHAR literal to BLOB cast' as test_case;

SELECT cast('中文测试' as blob) as blob_val;

CREATE TABLE test_str (ts TIMESTAMP, id INT, vc_data VARCHAR(100), label VARCHAR(50));
INSERT INTO test_str VALUES (NOW, 1, 'test data', 'str1');

SELECT 'Test 2: VARCHAR column to BLOB cast' as test_case;
SELECT id, cast(vc_data as blob) as blob_result FROM test_str WHERE id = 1;

-- ============================================
-- Test 3: BLOB to VARCHAR Cast
-- ============================================
DROP DATABASE IF EXISTS db_blob_cast_b2v;
CREATE DATABASE db_blob_cast_b2v;
USE db_blob_cast_b2v;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine', 'test2');
INSERT INTO test_blob VALUES (NOW, 3, '1234567890', 'test3');

SELECT 'Test 3: BLOB to VARCHAR cast' as test_case;
SELECT id, cast(b_data as varchar) as varchar_result FROM test_blob WHERE id = 1;

-- ============================================
-- Test 4: BLOB to BLOB Cast
-- ============================================
DROP DATABASE IF EXISTS db_blob_cast_b2b;
CREATE DATABASE db_blob_cast_b2b;
USE db_blob_cast_b2b;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');

SELECT 'Test 4: BLOB to BLOB cast (same type)' as test_case;
SELECT id, cast(b_data as blob) as blob_result FROM test_blob WHERE id = 1;

-- ============================================
-- Test 5: SUBSTR with Positive Position
-- ============================================
DROP DATABASE IF EXISTS db_blob_substr_pos;
CREATE DATABASE db_blob_substr_pos;
USE db_blob_substr_pos;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine test', 'test2');

SELECT 'Test 5: SUBSTR with positive position' as test_case;
SELECT id, substr(b_data, 1, 5) as result FROM test_blob WHERE id = 1;
SELECT id, substr(b_data, 7, 5) as result FROM test_blob WHERE id = 2;

-- ============================================
-- Test 6: SUBSTR with Negative Position
-- ============================================
DROP DATABASE IF EXISTS db_blob_substr_neg;
CREATE DATABASE db_blob_substr_neg;
USE db_blob_substr_neg;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine is great', 'test2');

SELECT 'Test 6: SUBSTR with negative position' as test_case;
SELECT id, substr(b_data, -6) as result FROM test_blob WHERE id = 1;
SELECT id, substr(b_data, -10, 5) as result FROM test_blob WHERE id = 2;

-- ============================================
-- Test 7: SUBSTR on Chinese BLOB
-- ============================================
DROP DATABASE IF EXISTS db_blob_substr_cn;
CREATE DATABASE db_blob_substr_cn;
USE db_blob_substr_cn;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, '中文测试数据', 'test1');

SELECT 'Test 7: SUBSTR on Chinese BLOB (byte positions)' as test_case;
SELECT id, substr(b_data, 1, 6) as result FROM test_blob WHERE id = 1;
SELECT id, substr(b_data, 7, 12) as result FROM test_blob WHERE id = 1;

-- ============================================
-- Test 8: BLOB NULL Handling
-- ============================================
DROP DATABASE IF EXISTS db_blob_null;
CREATE DATABASE db_blob_null;
USE db_blob_null;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 100, NULL, 'null_test');

SELECT 'Test 8: SUBSTR on NULL' as test_case;
SELECT id, substr(b_data, 1, 5) as result FROM test_blob WHERE id = 100;

SELECT 'Test 8: length on NULL' as test_case;
SELECT id, length(b_data) as len FROM test_blob WHERE id = 100;

SELECT 'Test 8: cast on NULL' as test_case;
SELECT id, cast(b_data as varchar) as result FROM test_blob WHERE id = 100;

-- ============================================
-- Test 9: Empty BLOB
-- ============================================
DROP DATABASE IF EXISTS db_blob_empty;
CREATE DATABASE db_blob_empty;
USE db_blob_empty;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 101, '', 'empty_test');

SELECT 'Test 9: length on empty BLOB' as test_case;
SELECT id, length(b_data) as len FROM test_blob WHERE id = 101;

SELECT 'Test 9: substr on empty BLOB' as test_case;
SELECT id, substr(b_data, 1, 5) as result FROM test_blob WHERE id = 101;

-- ============================================
-- Test 10: Single Byte BLOB
-- ============================================
DROP DATABASE IF EXISTS db_blob_single;
CREATE DATABASE db_blob_single;
USE db_blob_single;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 102, 'a', 'single_byte');

SELECT 'Test 10: substr on single byte' as test_case;
SELECT id, substr(b_data, 1, 1) as result FROM test_blob WHERE id = 102;

SELECT 'Test 10: length on single byte' as test_case;
SELECT id, length(b_data) as len FROM test_blob WHERE id = 102;

-- ============================================
-- Test 11: BLOB in WHERE Clause
-- ============================================
DROP DATABASE IF EXISTS db_blob_where;
CREATE DATABASE db_blob_where;
USE db_blob_where;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine', 'test2');

SELECT 'Test 11: BLOB in WHERE clause with CAST' as test_case;
SELECT ts, id, b_data FROM test_blob WHERE cast(b_data as varchar) = 'hello world';

-- ============================================
-- Test 12: BLOB with Aggregate Functions
-- ============================================
DROP DATABASE IF EXISTS db_blob_agg;
CREATE DATABASE db_blob_agg;
USE db_blob_agg;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'TDengine', 'test2');
INSERT INTO test_blob VALUES (NOW, 3, '1234567890', 'test3');

SELECT 'Test 12: COUNT with BLOB' as test_case;
SELECT count(*) FROM test_blob WHERE id <= 3;

SELECT 'Test 12: AVG/MAX/MIN with LENGTH' as test_case;
SELECT avg(length(b_data)), max(length(b_data)), min(length(b_data)) FROM test_blob WHERE id <= 3;

-- ============================================
-- Test 13: BLOB with GROUP BY
-- ============================================
DROP DATABASE IF EXISTS db_blob_group;
CREATE DATABASE db_blob_group;
USE db_blob_group;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');
INSERT INTO test_blob VALUES (NOW, 2, 'hello there', 'test2');
INSERT INTO test_blob VALUES (NOW, 3, 'TDengine', 'test3');

SELECT 'Test 13: GROUP BY with SUBSTR' as test_case;


-- ============================================
-- Test 14: BLOB Boundary Conditions
-- ============================================
DROP DATABASE IF EXISTS db_blob_boundary;
CREATE DATABASE db_blob_boundary;
USE db_blob_boundary;

CREATE TABLE test_blob (ts TIMESTAMP, id INT, b_data BLOB, label VARCHAR(50));
INSERT INTO test_blob VALUES (NOW, 1, 'hello world', 'test1');

SELECT 'Test 14: substr with position 0' as test_case;
SELECT id, substr(b_data, 0, 5) as result FROM test_blob WHERE id = 1;

SELECT 'Test 14: substr with out of range position' as test_case;
SELECT id, substr(b_data, 100) as result FROM test_blob WHERE id = 1;

SELECT 'Test 14: substr with length 0' as test_case;
SELECT id, substr(b_data, 1, 0) as result FROM test_blob WHERE id = 1;

SELECT 'Test 14: substr with negative length' as test_case;
SELECT id, substr(b_data, 1, -1) as result FROM test_blob WHERE id = 1;

SELECT 'Test 14: substr with very large length' as test_case;
SELECT id, substr(b_data, 1, 1000) as result FROM test_blob WHERE id = 1;

-- ============================================
-- Test Summary
-- ============================================
SELECT '============================================' as summary;
SELECT 'All BLOB function tests completed' as final_status;
SELECT 'Please verify results above' as reminder;

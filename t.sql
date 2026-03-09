DROP DATABASE IF EXISTS test_vtable_ref;
CREATE DATABASE test_vtable_ref;
USE test_vtable_ref;

-- ============================================================
-- 1. Create physical tables and insert data
-- ============================================================

CREATE TABLE org_ntb (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_col binary(32), nchar_col nchar(32));

INSERT INTO org_ntb VALUES ('2025-01-01 00:00:00', 0, 0, 0.0, 0.0, true, 'binary_0', 'nchar_0');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:01', 1, 100, 1.1, 2.2, false, 'binary_1', 'nchar_1');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:02', 2, 200, 2.2, 4.4, true, 'binary_2', 'nchar_2');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:03', 3, 300, 3.3, 6.6, false, 'binary_3', 'nchar_3');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:04', 4, 400, 4.4, 8.8, true, 'binary_4', 'nchar_4');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:05', 5, 500, 5.5, 11.0, false, 'binary_5', 'nchar_5');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:06', 6, 600, 6.6, 13.2, true, 'binary_6', 'nchar_6');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:07', 7, 700, 7.7, 15.4, false, 'binary_7', 'nchar_7');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:08', 8, 800, 8.8, 17.6, true, 'binary_8', 'nchar_8');
INSERT INTO org_ntb VALUES ('2025-01-01 00:00:09', 9, 900, 9.9, 19.8, false, 'binary_9', 'nchar_9');

CREATE TABLE org_ntb2 (ts timestamp, val int, info binary(32));
INSERT INTO org_ntb2 VALUES ('2025-01-01 00:00:00', 10, 'info_0');
INSERT INTO org_ntb2 VALUES ('2025-01-01 00:00:01', 20, 'info_1');
INSERT INTO org_ntb2 VALUES ('2025-01-01 00:00:02', 30, 'info_2');

CREATE STABLE org_stb (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16));
CREATE TABLE org_ctb_0 USING org_stb TAGS (0, 'tag_0');
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:00', 0, 0, 0.0, 0.0);
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:01', 1, 100, 1.1, 2.2);
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:02', 2, 200, 2.2, 4.4);
CREATE TABLE org_ctb_1 USING org_stb TAGS (1, 'tag_1');
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:00', 100, 10000, 110.0, 220.0);
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:01', 101, 10100, 111.1, 222.2);
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:02', 102, 10200, 112.2, 224.4);

-- ============================================================
-- 2. Layer 1: virtual normal table -> physical table
-- ============================================================

SELECT '=== 2. Create vntb1: vtable -> physical ===' AS test;
CREATE VTABLE vntb1 (ts timestamp, int_col int from org_ntb.int_col, bigint_col bigint from org_ntb.bigint_col, float_col float from org_ntb.float_col, double_col double from org_ntb.double_col);

SELECT * FROM vntb1 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vntb1;
SELECT * FROM vntb1 WHERE int_col > 5 ORDER BY ts;
SELECT * FROM vntb1 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;

-- ============================================================
-- 3. Layer 2: virtual normal table -> virtual normal table -> physical
-- ============================================================

SELECT '=== 3. Create vntb2: vtable -> vtable -> physical ===' AS test;
CREATE VTABLE vntb2 (ts timestamp, int_col int from vntb1.int_col, bigint_col bigint from vntb1.bigint_col, float_col float from vntb1.float_col, double_col double from vntb1.double_col);

SELECT * FROM vntb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vntb2;
SELECT * FROM vntb2 WHERE int_col > 5 ORDER BY ts;
SELECT * FROM vntb2 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;
SELECT MIN(int_col) AS min_val, MAX(int_col) AS max_val, FIRST(int_col) AS first_val, LAST(int_col) AS last_val FROM vntb2;

-- ============================================================
-- 4. Virtual super table with physical child table refs
-- ============================================================

SELECT '=== 4. Create vstb + vctb_0/vctb_1: vstb -> physical ===' AS test;
CREATE STABLE vstb (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1;
CREATE VTABLE vctb_0 (int_col from org_ctb_0.int_col, bigint_col from org_ctb_0.bigint_col, float_col from org_ctb_0.float_col, double_col from org_ctb_0.double_col) USING vstb TAGS (0, 'vtag_0');
CREATE VTABLE vctb_1 (int_col from org_ctb_1.int_col, bigint_col from org_ctb_1.bigint_col, float_col from org_ctb_1.float_col, double_col from org_ctb_1.double_col) USING vstb TAGS (1, 'vtag_1');

SELECT '--- vctb_0 (child -> physical org_ctb_0) ---' AS test;
SELECT * FROM vctb_0 ORDER BY ts;
SELECT '--- vctb_1 (child -> physical org_ctb_1) ---' AS test;
SELECT * FROM vctb_1 ORDER BY ts;
SELECT '--- vstb (super table scan, 2 child tables) ---' AS test;
SELECT * FROM vstb ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb;
SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb GROUP BY int_tag ORDER BY int_tag;
SELECT * FROM vstb WHERE int_tag = 0 ORDER BY ts;
SELECT * FROM vstb WHERE int_tag = 1 ORDER BY ts;

-- ============================================================
-- 5. Virtual super table with vtable child table refs (key test)
-- ============================================================

SELECT '=== 5. Create vstb2 + vctb2_0: vstb -> vtable -> physical ===' AS test;
CREATE STABLE vstb2 (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1;
CREATE VTABLE vctb2_0 (int_col from vntb1.int_col, bigint_col from vntb1.bigint_col, float_col from vntb1.float_col, double_col from vntb1.double_col) USING vstb2 TAGS (0, 'vtag2_0');

SELECT '--- vctb2_0 (child -> vtable vntb1 -> physical org_ntb) ---' AS test;
SELECT * FROM vctb2_0 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vctb2_0;
SELECT '--- vstb2 (super table scan, 1 child table -> vtable) ---' AS test;
SELECT * FROM vstb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vstb2;
SELECT * FROM vstb2 WHERE int_col >= 5 ORDER BY ts;
SELECT * FROM vstb2 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;

-- ============================================================
-- 6. Multiple child tables under vstb2, referencing different vtables
-- ============================================================

SELECT '=== 6. Add vctb2_1 referencing org_ctb_1 via vtable chain ===' AS test;
CREATE VTABLE vntb_from_ctb1 (ts timestamp, int_col int from org_ctb_1.int_col, bigint_col bigint from org_ctb_1.bigint_col, float_col float from org_ctb_1.float_col, double_col double from org_ctb_1.double_col);
SELECT '--- vntb_from_ctb1 (vtable -> physical org_ctb_1) ---' AS test;
SELECT * FROM vntb_from_ctb1 ORDER BY ts;

CREATE VTABLE vctb2_1 (int_col from vntb_from_ctb1.int_col, bigint_col from vntb_from_ctb1.bigint_col, float_col from vntb_from_ctb1.float_col, double_col from vntb_from_ctb1.double_col) USING vstb2 TAGS (1, 'vtag2_1');
SELECT '--- vctb2_1 (child -> vtable vntb_from_ctb1 -> physical org_ctb_1) ---' AS test;
SELECT * FROM vctb2_1 ORDER BY ts;

SELECT '--- vstb2 (super table, 2 children: vctb2_0 -> vntb1, vctb2_1 -> vntb_from_ctb1) ---' AS test;
SELECT * FROM vstb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2;
SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 GROUP BY int_tag ORDER BY int_tag;

-- ============================================================
-- 7. Partial column reference: vtable references subset of columns
-- ============================================================

SELECT '=== 7. Partial column vtable ===' AS test;
CREATE VTABLE vntb_partial (ts timestamp, int_col int from org_ntb.int_col, double_col double from org_ntb.double_col);
CREATE VTABLE vntb_partial2 (ts timestamp, int_col int from vntb_partial.int_col, double_col double from vntb_partial.double_col);
SELECT * FROM vntb_partial2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vntb_partial2;

-- ============================================================
-- 8. Aggregation queries on vstb2
-- ============================================================

SELECT '=== 8. Aggregation on vstb2 ===' AS test;
SELECT MIN(int_col) AS min_val, MAX(int_col) AS max_val FROM vstb2;
SELECT FIRST(int_col) AS first_val, LAST(int_col) AS last_val FROM vstb2;
SELECT int_tag, MIN(int_col) AS min_val, MAX(int_col) AS max_val FROM vstb2 GROUP BY int_tag ORDER BY int_tag;

-- ============================================================
-- 9. Verify data consistency: vntb2 should return same data as org_ntb
-- ============================================================

SELECT '=== 9. Data consistency check ===' AS test;
SELECT '--- org_ntb row count ---' AS test;
SELECT COUNT(*) FROM org_ntb;
SELECT '--- vntb1 row count (should = org_ntb) ---' AS test;
SELECT COUNT(*) FROM vntb1;
SELECT '--- vntb2 row count (should = org_ntb) ---' AS test;
SELECT COUNT(*) FROM vntb2;
SELECT '--- vctb2_0 row count (should = org_ntb = 10) ---' AS test;
SELECT COUNT(*) FROM vctb2_0;
SELECT '--- vctb2_1 row count (should = org_ctb_1 = 3) ---' AS test;
SELECT COUNT(*) FROM vctb2_1;
SELECT '--- vstb2 row count (should = 10 + 3 = 13) ---' AS test;
SELECT COUNT(*) FROM vstb2;

-- ============================================================
-- 10. Virtual super table with child referencing vtable -> vtable (2 layers)
-- ============================================================

SELECT '=== 10. vstb3: child -> vntb2 -> vntb1 -> org_ntb (2-layer vtable chain) ===' AS test;
CREATE STABLE vstb3 (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int) VIRTUAL 1;
CREATE VTABLE vctb3_0 (int_col from vntb2.int_col, bigint_col from vntb2.bigint_col, float_col from vntb2.float_col, double_col from vntb2.double_col) USING vstb3 TAGS (0);
SELECT '--- vctb3_0 (child -> vntb2 -> vntb1 -> org_ntb) ---' AS test;
SELECT * FROM vctb3_0 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vctb3_0;
SELECT '--- vstb3 super table scan ---' AS test;
SELECT * FROM vstb3 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb3;

-- ============================================================
-- 11. Window queries on vtable-ref-vtable
-- ============================================================

SELECT '=== 11. Interval window on vntb2 ===' AS test;
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb2 INTERVAL(3s);

SELECT '=== 11b. Interval window on vstb2 ===' AS test;
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 INTERVAL(3s);

-- ============================================================
-- 12. LIMIT / OFFSET
-- ============================================================

SELECT '=== 12. LIMIT/OFFSET ===' AS test;
SELECT * FROM vntb2 ORDER BY ts LIMIT 3;
SELECT * FROM vntb2 ORDER BY ts LIMIT 3 OFFSET 5;
SELECT * FROM vstb2 ORDER BY ts LIMIT 5;

-- ============================================================
-- 13. ORDER BY non-ts column
-- ============================================================

SELECT '=== 13. ORDER BY int_col DESC ===' AS test;
SELECT * FROM vntb2 ORDER BY int_col DESC LIMIT 5;
SELECT * FROM vstb2 ORDER BY int_col DESC LIMIT 5;

-- ============================================================
-- DONE
-- ============================================================
SELECT '=== ALL TESTS COMPLETED ===' AS test;

DROP DATABASE IF EXISTS test_vtable_ref;
CREATE DATABASE test_vtable_ref VGROUPS 4;
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

CREATE TABLE org_ntb_empty (ts timestamp, int_col int, double_col double);

CREATE TABLE org_ntb_null (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double, binary_col binary(32));
INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:00', NULL, 100, NULL, 1.1, NULL);
INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:01', 1, NULL, 2.2, NULL, 'val_1');
INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:02', NULL, NULL, NULL, NULL, NULL);
INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:03', 3, 300, 3.3, 6.6, 'val_3');

CREATE STABLE org_stb (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16));
CREATE TABLE org_ctb_0 USING org_stb TAGS (0, 'tag_0');
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:00', 0, 0, 0.0, 0.0);
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:01', 1, 100, 1.1, 2.2);
INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:02', 2, 200, 2.2, 4.4);
CREATE TABLE org_ctb_1 USING org_stb TAGS (1, 'tag_1');
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:00', 100, 10000, 110.0, 220.0);
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:01', 101, 10100, 111.1, 222.2);
INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:02', 102, 10200, 112.2, 224.4);
CREATE TABLE org_ctb_2 USING org_stb TAGS (2, 'tag_2');
INSERT INTO org_ctb_2 VALUES ('2025-01-01 00:00:00', 200, 20000, 210.0, 420.0);
INSERT INTO org_ctb_2 VALUES ('2025-01-01 00:00:01', 201, 20100, 211.1, 422.2);

-- ============================================================
-- 2. Layer 1: virtual normal table -> physical table
-- ============================================================

SELECT '=== 2. vntb1: vtable -> physical ===' AS test;
CREATE VTABLE vntb1 (ts timestamp, int_col int from org_ntb.int_col, bigint_col bigint from org_ntb.bigint_col, float_col float from org_ntb.float_col, double_col double from org_ntb.double_col);

SELECT * FROM vntb1 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vntb1;
SELECT * FROM vntb1 WHERE int_col > 5 ORDER BY ts;
SELECT * FROM vntb1 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;

-- ============================================================
-- 3. Layer 2: virtual normal table -> virtual normal table -> physical
-- ============================================================

SELECT '=== 3. vntb2: vtable -> vtable -> physical ===' AS test;
CREATE VTABLE vntb2 (ts timestamp, int_col int from vntb1.int_col, bigint_col bigint from vntb1.bigint_col, float_col float from vntb1.float_col, double_col double from vntb1.double_col);

SELECT * FROM vntb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vntb2;
SELECT * FROM vntb2 WHERE int_col > 5 ORDER BY ts;
SELECT * FROM vntb2 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;
SELECT MIN(int_col) AS min_val, MAX(int_col) AS max_val, FIRST(int_col) AS first_val, LAST(int_col) AS last_val FROM vntb2;

-- ============================================================
-- 4. Virtual super table with physical child table refs
-- ============================================================

SELECT '=== 4. vstb + vctb_0/vctb_1: vstb -> physical ===' AS test;
CREATE STABLE vstb (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1;
CREATE VTABLE vctb_0 (int_col from org_ctb_0.int_col, bigint_col from org_ctb_0.bigint_col, float_col from org_ctb_0.float_col, double_col from org_ctb_0.double_col) USING vstb TAGS (0, 'vtag_0');
CREATE VTABLE vctb_1 (int_col from org_ctb_1.int_col, bigint_col from org_ctb_1.bigint_col, float_col from org_ctb_1.float_col, double_col from org_ctb_1.double_col) USING vstb TAGS (1, 'vtag_1');

SELECT '--- vctb_0 ---' AS test;
SELECT * FROM vctb_0 ORDER BY ts;
SELECT '--- vctb_1 ---' AS test;
SELECT * FROM vctb_1 ORDER BY ts;
SELECT '--- vstb scan ---' AS test;
SELECT * FROM vstb ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb;
SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb GROUP BY int_tag ORDER BY int_tag;
SELECT * FROM vstb WHERE int_tag = 0 ORDER BY ts;
SELECT * FROM vstb WHERE int_tag = 1 ORDER BY ts;

-- ============================================================
-- 5. Virtual super table with vtable child table refs (key test)
-- ============================================================

SELECT '=== 5. vstb2 + vctb2_0: vstb -> vtable -> physical ===' AS test;
CREATE STABLE vstb2 (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1;
CREATE VTABLE vctb2_0 (int_col from vntb1.int_col, bigint_col from vntb1.bigint_col, float_col from vntb1.float_col, double_col from vntb1.double_col) USING vstb2 TAGS (0, 'vtag2_0');

SELECT '--- vctb2_0 (child -> vntb1 -> org_ntb) ---' AS test;
SELECT * FROM vctb2_0 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vctb2_0;
SELECT '--- vstb2 scan (1 child) ---' AS test;
SELECT * FROM vstb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vstb2;
SELECT * FROM vstb2 WHERE int_col >= 5 ORDER BY ts;
SELECT * FROM vstb2 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts;

-- ============================================================
-- 6. Multiple child tables under vstb2 referencing different vtables
-- ============================================================

SELECT '=== 6. vctb2_1 referencing org_ctb_1 via vtable chain ===' AS test;
CREATE VTABLE vntb_from_ctb1 (ts timestamp, int_col int from org_ctb_1.int_col, bigint_col bigint from org_ctb_1.bigint_col, float_col float from org_ctb_1.float_col, double_col double from org_ctb_1.double_col);
SELECT * FROM vntb_from_ctb1 ORDER BY ts;

CREATE VTABLE vctb2_1 (int_col from vntb_from_ctb1.int_col, bigint_col from vntb_from_ctb1.bigint_col, float_col from vntb_from_ctb1.float_col, double_col from vntb_from_ctb1.double_col) USING vstb2 TAGS (1, 'vtag2_1');
SELECT * FROM vctb2_1 ORDER BY ts;

SELECT '--- vstb2 scan (2 children) ---' AS test;
SELECT * FROM vstb2 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2;
SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 GROUP BY int_tag ORDER BY int_tag;

-- ============================================================
-- 7. Partial column reference: vtable refs subset of columns
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
-- 9. Data consistency check
-- ============================================================

SELECT '=== 9. Data consistency ===' AS test;
SELECT '--- org_ntb=10 ---' AS test;
SELECT COUNT(*) FROM org_ntb;
SELECT '--- vntb1=10 ---' AS test;
SELECT COUNT(*) FROM vntb1;
SELECT '--- vntb2=10 ---' AS test;
SELECT COUNT(*) FROM vntb2;
SELECT '--- vctb2_0=10 ---' AS test;
SELECT COUNT(*) FROM vctb2_0;
SELECT '--- vctb2_1=3 ---' AS test;
SELECT COUNT(*) FROM vctb2_1;
SELECT '--- vstb2=13 ---' AS test;
SELECT COUNT(*) FROM vstb2;

-- ============================================================
-- 10. 2-layer chain: vstb3 child -> vntb2 -> vntb1 -> org_ntb
-- ============================================================

SELECT '=== 10. vstb3: 2-layer vtable chain ===' AS test;
CREATE STABLE vstb3 (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (int_tag int) VIRTUAL 1;
CREATE VTABLE vctb3_0 (int_col from vntb2.int_col, bigint_col from vntb2.bigint_col, float_col from vntb2.float_col, double_col from vntb2.double_col) USING vstb3 TAGS (0);
SELECT * FROM vctb3_0 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vctb3_0;
SELECT '--- vstb3 super table scan ---' AS test;
SELECT * FROM vstb3 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb3;

-- ============================================================
-- 11. Window queries
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
SELECT * FROM vstb2 ORDER BY ts LIMIT 5 OFFSET 8;

-- ============================================================
-- 13. ORDER BY non-ts column
-- ============================================================

SELECT '=== 13. ORDER BY int_col DESC ===' AS test;
SELECT * FROM vntb2 ORDER BY int_col DESC LIMIT 5;
SELECT * FROM vstb2 ORDER BY int_col DESC LIMIT 5;

-- ============================================================
-- 14. All data types through vtable chain (bool, binary, nchar)
-- ============================================================

SELECT '=== 14. All data types vtable chain ===' AS test;
CREATE VTABLE vntb_alltype (ts timestamp, int_col int from org_ntb.int_col, bigint_col bigint from org_ntb.bigint_col, float_col float from org_ntb.float_col, double_col double from org_ntb.double_col, bool_col bool from org_ntb.bool_col, binary_col binary(32) from org_ntb.binary_col, nchar_col nchar(32) from org_ntb.nchar_col);
SELECT * FROM vntb_alltype ORDER BY ts LIMIT 5;

CREATE VTABLE vntb_alltype2 (ts timestamp, int_col int from vntb_alltype.int_col, bigint_col bigint from vntb_alltype.bigint_col, float_col float from vntb_alltype.float_col, double_col double from vntb_alltype.double_col, bool_col bool from vntb_alltype.bool_col, binary_col binary(32) from vntb_alltype.binary_col, nchar_col nchar(32) from vntb_alltype.nchar_col);
SELECT * FROM vntb_alltype2 ORDER BY ts LIMIT 5;

SELECT '--- data type consistency: vntb_alltype2 vs org_ntb ---' AS test;
SELECT COUNT(*) FROM vntb_alltype2;
SELECT bool_col, COUNT(*) AS cnt FROM vntb_alltype2 GROUP BY bool_col ORDER BY bool_col;
SELECT * FROM vntb_alltype2 WHERE binary_col = 'binary_3';
SELECT * FROM vntb_alltype2 WHERE nchar_col = 'nchar_7';
SELECT * FROM vntb_alltype2 WHERE bool_col = true ORDER BY ts LIMIT 3;

-- ============================================================
-- 15. NULL value propagation through vtable chain
-- ============================================================

SELECT '=== 15. NULL value propagation ===' AS test;
CREATE VTABLE vntb_null1 (ts timestamp, int_col int from org_ntb_null.int_col, bigint_col bigint from org_ntb_null.bigint_col, float_col float from org_ntb_null.float_col, double_col double from org_ntb_null.double_col, binary_col binary(32) from org_ntb_null.binary_col);
SELECT * FROM vntb_null1 ORDER BY ts;

CREATE VTABLE vntb_null2 (ts timestamp, int_col int from vntb_null1.int_col, bigint_col bigint from vntb_null1.bigint_col, float_col float from vntb_null1.float_col, double_col double from vntb_null1.double_col, binary_col binary(32) from vntb_null1.binary_col);
SELECT '--- vntb_null2: NULLs preserved through chain ---' AS test;
SELECT * FROM vntb_null2 ORDER BY ts;
SELECT COUNT(*) AS total, COUNT(int_col) AS non_null_int, COUNT(binary_col) AS non_null_bin FROM vntb_null2;
SELECT * FROM vntb_null2 WHERE int_col IS NULL ORDER BY ts;
SELECT * FROM vntb_null2 WHERE int_col IS NOT NULL ORDER BY ts;
SELECT AVG(int_col) AS avg_int FROM vntb_null2;
SELECT SUM(bigint_col) AS sum_bg FROM vntb_null2;

-- ============================================================
-- 16. Empty source table through vtable chain
-- ============================================================

SELECT '=== 16. Empty source table ===' AS test;
CREATE VTABLE vntb_empty1 (ts timestamp, int_col int from org_ntb_empty.int_col, double_col double from org_ntb_empty.double_col);
CREATE VTABLE vntb_empty2 (ts timestamp, int_col int from vntb_empty1.int_col, double_col double from vntb_empty1.double_col);
SELECT * FROM vntb_empty1;
SELECT * FROM vntb_empty2;
SELECT COUNT(*) FROM vntb_empty2;

CREATE STABLE vstb_empty (ts timestamp, int_col int, double_col double) TAGS (t1 int) VIRTUAL 1;
CREATE VTABLE vctb_empty_0 (int_col from vntb_empty1.int_col, double_col from vntb_empty1.double_col) USING vstb_empty TAGS (0);
SELECT * FROM vstb_empty;
SELECT COUNT(*) FROM vstb_empty;

-- ============================================================
-- 17. Mixed references: same vstb with children referencing
--     both physical tables and virtual tables
-- ============================================================

SELECT '=== 17. Mixed physical + vtable refs under same vstb ===' AS test;
CREATE STABLE vstb_mix (ts timestamp, int_col int, bigint_col bigint, float_col float, double_col double) TAGS (src_type int, src_name binary(32)) VIRTUAL 1;
CREATE VTABLE vctb_mix_phys (int_col from org_ctb_0.int_col, bigint_col from org_ctb_0.bigint_col, float_col from org_ctb_0.float_col, double_col from org_ctb_0.double_col) USING vstb_mix TAGS (0, 'physical');
CREATE VTABLE vctb_mix_virt (int_col from vntb1.int_col, bigint_col from vntb1.bigint_col, float_col from vntb1.float_col, double_col from vntb1.double_col) USING vstb_mix TAGS (1, 'vtable_L1');
CREATE VTABLE vctb_mix_virt2 (int_col from vntb2.int_col, bigint_col from vntb2.bigint_col, float_col from vntb2.float_col, double_col from vntb2.double_col) USING vstb_mix TAGS (2, 'vtable_L2');

SELECT '--- vstb_mix scan: 3 children, mixed sources ---' AS test;
SELECT * FROM vstb_mix ORDER BY ts, src_type LIMIT 15;
SELECT src_type, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb_mix GROUP BY src_type ORDER BY src_type;
SELECT COUNT(*) FROM vstb_mix;

-- ============================================================
-- 18. Multiple vstbs with children referencing the SAME vtable
-- ============================================================

SELECT '=== 18. Multiple vstbs sharing same vtable source ===' AS test;
CREATE STABLE vstb_share_a (ts timestamp, int_col int, double_col double) TAGS (t1 int) VIRTUAL 1;
CREATE STABLE vstb_share_b (ts timestamp, int_col int, double_col double) TAGS (t1 int) VIRTUAL 1;
CREATE VTABLE vctb_share_a0 (int_col from vntb1.int_col, double_col from vntb1.double_col) USING vstb_share_a TAGS (0);
CREATE VTABLE vctb_share_b0 (int_col from vntb1.int_col, double_col from vntb1.double_col) USING vstb_share_b TAGS (0);

SELECT '--- vstb_share_a ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vstb_share_a;
SELECT '--- vstb_share_b ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vstb_share_b;

-- ============================================================
-- 19. TAG filter / TBNAME filter / PARTITION BY
-- ============================================================

SELECT '=== 19. TAG/TBNAME/PARTITION BY on vstb2 ===' AS test;
SELECT * FROM vstb2 WHERE int_tag = 0 ORDER BY ts LIMIT 5;
SELECT * FROM vstb2 WHERE int_tag = 1 ORDER BY ts;
SELECT * FROM vstb2 WHERE binary_tag = 'vtag2_0' ORDER BY ts LIMIT 3;
SELECT * FROM vstb2 WHERE int_tag IN (0, 1) ORDER BY ts LIMIT 5;
SELECT TBNAME, COUNT(*) AS cnt FROM vstb2 PARTITION BY TBNAME;
SELECT int_tag, COUNT(*) AS cnt, AVG(int_col) AS avg_int FROM vstb2 PARTITION BY int_tag;

-- ============================================================
-- 20. Three children under one vstb, each via different chains
-- ============================================================

SELECT '=== 20. vstb with 3 children via different vtable chains ===' AS test;
CREATE VTABLE vntb_from_ctb2 (ts timestamp, int_col int from org_ctb_2.int_col, bigint_col bigint from org_ctb_2.bigint_col, float_col float from org_ctb_2.float_col, double_col double from org_ctb_2.double_col);
CREATE VTABLE vctb2_2 (int_col from vntb_from_ctb2.int_col, bigint_col from vntb_from_ctb2.bigint_col, float_col from vntb_from_ctb2.float_col, double_col from vntb_from_ctb2.double_col) USING vstb2 TAGS (2, 'vtag2_2');

SELECT '--- vstb2 now has 3 children ---' AS test;
SELECT * FROM vstb2 ORDER BY ts, int_tag;
SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 GROUP BY int_tag ORDER BY int_tag;
SELECT COUNT(*) FROM vstb2;

-- ============================================================
-- 21. Subquery / Nested query
-- ============================================================

SELECT '=== 21. Subqueries on vtable chain ===' AS test;
SELECT * FROM (SELECT ts, int_col, double_col FROM vntb2 WHERE int_col > 3) ORDER BY ts;
SELECT * FROM (SELECT int_tag, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 GROUP BY int_tag) ORDER BY int_tag;
SELECT * FROM (SELECT * FROM vstb2 WHERE int_tag = 0 ORDER BY ts) LIMIT 3;
SELECT AVG(int_col) FROM (SELECT int_col FROM vntb2 WHERE int_col BETWEEN 2 AND 7);

-- ============================================================
-- 22. UNION / UNION ALL
-- ============================================================

SELECT '=== 22. UNION queries ===' AS test;
SELECT int_col FROM vntb1 WHERE int_col <= 2 UNION ALL SELECT int_col FROM vntb2 WHERE int_col >= 8 ORDER BY int_col;
SELECT ts, int_col FROM vctb2_0 WHERE int_col < 3 UNION ALL SELECT ts, int_col FROM vctb2_1 WHERE int_col > 100 ORDER BY int_col;

-- ============================================================
-- 23. JOIN: vtable join vtable
-- ============================================================

SELECT '=== 23. JOIN queries ===' AS test;
SELECT a.ts, a.int_col AS a_int, b.int_col AS b_int FROM vntb1 a, vntb2 b WHERE a.ts = b.ts ORDER BY a.ts LIMIT 5;
SELECT a.ts, a.int_col, b.val FROM vntb1 a, org_ntb2 b WHERE a.ts = b.ts ORDER BY a.ts;

-- ============================================================
-- 24. Advanced functions on vtable chain
-- ============================================================

SELECT '=== 24. Advanced functions ===' AS test;
SELECT SPREAD(int_col) AS spread_int FROM vntb2;
SELECT SPREAD(int_col) AS spread_int FROM vstb2;
SELECT DIFF(int_col) AS diff_int FROM vntb2;
SELECT IRATE(int_col) FROM vntb2;
SELECT TOP(int_col, 3) FROM vntb2;
SELECT BOTTOM(int_col, 3) FROM vntb2;
SELECT TOP(int_col, 3) FROM vstb2;
SELECT BOTTOM(int_col, 3) FROM vstb2;
SELECT SAMPLE(int_col, 3) FROM vntb2;
SELECT UNIQUE(int_col) FROM vntb2 ORDER BY ts LIMIT 5;

-- ============================================================
-- 25. Interval window with FILL on vtable chain
-- ============================================================

SELECT '=== 25. Interval + FILL ===' AS test;
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb2 WHERE ts >= '2025-01-01 00:00:00' AND ts < '2025-01-01 00:00:10' INTERVAL(2s) FILL(VALUE, 0, 0);
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 WHERE ts >= '2025-01-01 00:00:00' AND ts < '2025-01-01 00:00:04' PARTITION BY int_tag INTERVAL(1s) FILL(PREV) SLIMIT 10;

-- ============================================================
-- 26. State window / Session window on vtable chain
-- ============================================================

SELECT '=== 26. State and Session windows ===' AS test;
SELECT _wstart, _wend, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb_alltype2 STATE_WINDOW(bool_col);
SELECT _wstart, _wend, COUNT(*) AS cnt FROM vntb_alltype2 SESSION(ts, 2s);

-- ============================================================
-- 27. DISTINCT on vtable chain
-- ============================================================

SELECT '=== 27. DISTINCT ===' AS test;
SELECT DISTINCT bool_col FROM vntb_alltype2;
SELECT DISTINCT int_tag FROM vstb2 ORDER BY int_tag;

-- ============================================================
-- 28. Deep vtable chain: 1 to 5 layers (TSDB_MAX_VTABLE_REF_DEPTH=5)
--     chain: org_ntb <- L1 <- L2 <- L3 <- L4 <- L5
--     L1=vntb1 (already created), L2=vntb2 (already created)
-- ============================================================

SELECT '=== 28a. Layer 3: vntb_L3 -> vntb2 -> vntb1 -> org_ntb ===' AS test;
CREATE VTABLE vntb_L3 (ts timestamp, int_col int from vntb2.int_col, bigint_col bigint from vntb2.bigint_col);
SELECT * FROM vntb_L3 ORDER BY ts LIMIT 5;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb_L3;

SELECT '=== 28b. Layer 4: vntb_L4 -> vntb_L3 -> vntb2 -> vntb1 -> org_ntb ===' AS test;
CREATE VTABLE vntb_L4 (ts timestamp, int_col int from vntb_L3.int_col, bigint_col bigint from vntb_L3.bigint_col);
SELECT * FROM vntb_L4 ORDER BY ts LIMIT 5;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb_L4;

SELECT '=== 28c. Layer 5: vntb_L5 -> vntb_L4 -> ... -> org_ntb ===' AS test;
CREATE VTABLE vntb_L5 (ts timestamp, int_col int from vntb_L4.int_col, bigint_col bigint from vntb_L4.bigint_col);
SELECT * FROM vntb_L5 ORDER BY ts LIMIT 5;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb_L5;
SELECT MIN(int_col) AS min_val, MAX(int_col) AS max_val FROM vntb_L5;
SELECT * FROM vntb_L5 WHERE int_col >= 7 ORDER BY ts;
SELECT AVG(int_col) FROM vntb_L5;

SELECT '=== 28d. Data consistency across all layers ===' AS test;
SELECT '--- org_ntb ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM org_ntb;
SELECT '--- vntb1 (L1) ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vntb1;
SELECT '--- vntb2 (L2) ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vntb2;
SELECT '--- vntb_L3 ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vntb_L3;
SELECT '--- vntb_L4 ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vntb_L4;
SELECT '--- vntb_L5 ---' AS test;
SELECT COUNT(*), SUM(int_col) FROM vntb_L5;

SELECT '=== 28e. vstb with child referencing deep chains ===' AS test;
CREATE STABLE vstb_deep (ts timestamp, int_col int, bigint_col bigint) TAGS (depth int) VIRTUAL 1;
CREATE VTABLE vctb_deep_L2 (int_col from vntb2.int_col, bigint_col from vntb2.bigint_col) USING vstb_deep TAGS (2);
CREATE VTABLE vctb_deep_L3 (int_col from vntb_L3.int_col, bigint_col from vntb_L3.bigint_col) USING vstb_deep TAGS (3);
CREATE VTABLE vctb_deep_L4 (int_col from vntb_L4.int_col, bigint_col from vntb_L4.bigint_col) USING vstb_deep TAGS (4);
SELECT '--- vstb_deep scan (3 children at depth 2/3/4) ---' AS test;
SELECT * FROM vstb_deep ORDER BY ts, depth LIMIT 15;
SELECT depth, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb_deep GROUP BY depth ORDER BY depth;
SELECT COUNT(*) FROM vstb_deep;

SELECT '=== 28f. Aggregation and window on deepest chain (L5) ===' AS test;
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb_L5 INTERVAL(3s);
SELECT FIRST(int_col), LAST(int_col) FROM vntb_L5;
SELECT SPREAD(int_col) FROM vntb_L5;
SELECT * FROM vntb_L5 ORDER BY int_col DESC LIMIT 3;

-- ============================================================
-- 29. Depth exceeded: layer 6 should fail (max=5)
-- ============================================================

SELECT '=== 29. Layer 6 creation should fail (depth exceeds limit of 5) ===' AS test;
CREATE VTABLE vntb_L6 (ts timestamp, int_col int from vntb_L5.int_col);

SELECT '=== 29a. vstb child referencing L5 should also fail (6th layer) ===' AS test;
CREATE VTABLE vctb_deep_L5_fail (int_col from vntb_L5.int_col, bigint_col from vntb_L5.bigint_col) USING vstb_deep TAGS (5);

-- ============================================================
-- 29b. Single-column vtable chain
-- ============================================================

SELECT '=== 29b. Single-column chain ===' AS test;
CREATE VTABLE vntb_1col_a (ts timestamp, int_col int from org_ntb.int_col);
CREATE VTABLE vntb_1col_b (ts timestamp, int_col int from vntb_1col_a.int_col);
SELECT * FROM vntb_1col_b ORDER BY ts;
SELECT COUNT(*), SUM(int_col) FROM vntb_1col_b;

-- ============================================================
-- 30. Verify vstb3 (2-layer chain) with aggregation + window
-- ============================================================

SELECT '=== 30. vstb3 advanced queries ===' AS test;
SELECT * FROM vstb3 ORDER BY ts;
SELECT COUNT(*) AS cnt, SUM(int_col) AS sum_int, AVG(double_col) AS avg_dbl FROM vstb3;
SELECT MIN(int_col), MAX(int_col), FIRST(int_col), LAST(int_col) FROM vstb3;
SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) FROM vstb3 INTERVAL(3s);
SELECT * FROM vstb3 WHERE int_col BETWEEN 3 AND 7 ORDER BY ts;
SELECT * FROM vstb3 ORDER BY int_col DESC LIMIT 3;

-- ============================================================
-- 31. LAST_ROW on vtable chain
-- ============================================================

SELECT '=== 31. LAST_ROW ===' AS test;
SELECT LAST_ROW(*) FROM vntb2;
SELECT LAST_ROW(*) FROM vstb2;
SELECT LAST_ROW(int_col) FROM vstb3;

-- ============================================================
-- 32. Multiple columns from different physical tables in one vtable
-- ============================================================

SELECT '=== 32. Cross-table column reference ===' AS test;
CREATE VTABLE vntb_cross (ts timestamp, a_int int from org_ntb.int_col, b_val int from org_ntb2.val);
SELECT * FROM vntb_cross ORDER BY ts;
CREATE VTABLE vntb_cross2 (ts timestamp, a_int int from vntb_cross.a_int, b_val int from vntb_cross.b_val);
SELECT '--- vntb_cross2: chain of cross-table refs ---' AS test;
SELECT * FROM vntb_cross2 ORDER BY ts;
SELECT COUNT(*), SUM(a_int), SUM(b_val) FROM vntb_cross2;

-- ============================================================
-- 33. Large number of child tables under vstb
-- ============================================================

SELECT '=== 33. Many children under one vstb ===' AS test;
CREATE STABLE org_stb_many (ts timestamp, val int) TAGS (idx int);
CREATE TABLE org_ctb_m0 USING org_stb_many TAGS (0);
CREATE TABLE org_ctb_m1 USING org_stb_many TAGS (1);
CREATE TABLE org_ctb_m2 USING org_stb_many TAGS (2);
CREATE TABLE org_ctb_m3 USING org_stb_many TAGS (3);
CREATE TABLE org_ctb_m4 USING org_stb_many TAGS (4);
INSERT INTO org_ctb_m0 VALUES ('2025-01-01 00:00:00', 10);
INSERT INTO org_ctb_m1 VALUES ('2025-01-01 00:00:00', 20);
INSERT INTO org_ctb_m2 VALUES ('2025-01-01 00:00:00', 30);
INSERT INTO org_ctb_m3 VALUES ('2025-01-01 00:00:00', 40);
INSERT INTO org_ctb_m4 VALUES ('2025-01-01 00:00:00', 50);

CREATE VTABLE vntb_m0 (ts timestamp, val int from org_ctb_m0.val);
CREATE VTABLE vntb_m1 (ts timestamp, val int from org_ctb_m1.val);
CREATE VTABLE vntb_m2 (ts timestamp, val int from org_ctb_m2.val);
CREATE VTABLE vntb_m3 (ts timestamp, val int from org_ctb_m3.val);
CREATE VTABLE vntb_m4 (ts timestamp, val int from org_ctb_m4.val);

CREATE STABLE vstb_many (ts timestamp, val int) TAGS (idx int) VIRTUAL 1;
CREATE VTABLE vctb_many_0 (val from vntb_m0.val) USING vstb_many TAGS (0);
CREATE VTABLE vctb_many_1 (val from vntb_m1.val) USING vstb_many TAGS (1);
CREATE VTABLE vctb_many_2 (val from vntb_m2.val) USING vstb_many TAGS (2);
CREATE VTABLE vctb_many_3 (val from vntb_m3.val) USING vstb_many TAGS (3);
CREATE VTABLE vctb_many_4 (val from vntb_m4.val) USING vstb_many TAGS (4);

SELECT '--- vstb_many: 5 children each via vtable chain ---' AS test;
SELECT * FROM vstb_many ORDER BY idx, ts;
SELECT COUNT(*) AS cnt, SUM(val) AS sum_val FROM vstb_many;
SELECT idx, val FROM vstb_many ORDER BY idx;

-- ============================================================
-- 34. SLIMIT / SOFFSET on vstb (partition limit)
-- ============================================================

SELECT '=== 34. SLIMIT/SOFFSET ===' AS test;
SELECT int_tag, COUNT(*) AS cnt FROM vstb2 PARTITION BY int_tag SLIMIT 2;
SELECT int_tag, COUNT(*) AS cnt FROM vstb2 PARTITION BY int_tag SLIMIT 1 SOFFSET 1;

-- ============================================================
-- 35. WHERE with complex conditions on vstb chain
-- ============================================================

SELECT '=== 35. Complex WHERE on vstb2 ===' AS test;
SELECT * FROM vstb2 WHERE int_col > 0 AND int_col < 5 ORDER BY ts;
SELECT * FROM vstb2 WHERE int_col > 100 OR int_col < 2 ORDER BY ts;
SELECT * FROM vstb2 WHERE int_tag = 0 AND int_col BETWEEN 3 AND 7 ORDER BY ts;
SELECT * FROM vstb2 WHERE ts >= '2025-01-01 00:00:01' AND ts <= '2025-01-01 00:00:02' AND int_tag = 1 ORDER BY ts;

-- ============================================================
-- 36. Arithmetic expressions in SELECT on vtable chain
-- ============================================================

SELECT '=== 36. Arithmetic expressions ===' AS test;
SELECT ts, int_col, int_col * 2 AS doubled, int_col + bigint_col AS combined FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, int_col, double_col, int_col + double_col AS sum_mixed FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, int_col * 10 + 5 AS expr FROM vstb2 WHERE int_tag = 0 ORDER BY ts LIMIT 5;

-- ============================================================
-- 37. CAST on vtable chain
-- ============================================================

SELECT '=== 37. CAST ===' AS test;
SELECT ts, CAST(int_col AS BIGINT) AS bg, CAST(int_col AS DOUBLE) AS dbl FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, CAST(float_col AS INT) AS truncated FROM vntb2 ORDER BY ts LIMIT 5;

-- ============================================================
-- 38. String functions on vtable chain (binary/nchar)
-- ============================================================

SELECT '=== 38. String functions on vtable chain ===' AS test;
SELECT ts, binary_col, LENGTH(binary_col) AS len FROM vntb_alltype2 ORDER BY ts LIMIT 5;
SELECT ts, nchar_col, LENGTH(nchar_col) AS len FROM vntb_alltype2 ORDER BY ts LIMIT 5;
SELECT ts, CONCAT(binary_col, '_suffix') AS appended FROM vntb_alltype2 ORDER BY ts LIMIT 5;
SELECT ts, LOWER(binary_col) AS lwr, UPPER(binary_col) AS upr FROM vntb_alltype2 ORDER BY ts LIMIT 3;
SELECT * FROM vntb_alltype2 WHERE binary_col LIKE 'binary_5%';

-- ============================================================
-- 39. Math functions on vtable chain
-- ============================================================

SELECT '=== 39. Math functions ===' AS test;
SELECT ts, ABS(int_col - 5) AS abs_val FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, CEIL(float_col) AS c, FLOOR(float_col) AS f, ROUND(float_col) AS r FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, LOG(double_col + 1, 10) AS log10_val FROM vntb2 WHERE double_col > 0 ORDER BY ts LIMIT 5;
SELECT ts, SQRT(double_col) AS sqrt_val FROM vntb2 WHERE double_col >= 0 ORDER BY ts LIMIT 5;
SELECT ts, POW(int_col, 2) AS squared FROM vntb2 ORDER BY ts LIMIT 5;

-- ============================================================
-- 40. Time functions on vtable chain
-- ============================================================

SELECT '=== 40. Time functions ===' AS test;
SELECT ts, TIMETRUNCATE(ts, 1s) AS trunc_s, TIMEDIFF(ts, '2025-01-01 00:00:00') AS diff_us FROM vntb2 ORDER BY ts LIMIT 5;
SELECT ts, TO_CHAR(ts, 'YYYY-MM-DD') AS date_str FROM vntb2 ORDER BY ts LIMIT 3;

-- ============================================================
-- 41. DROP source vtable, query should fail or return empty
-- ============================================================

SELECT '=== 41. DROP intermediate vtable ===' AS test;
SELECT '--- create temp chain: org_ntb -> vntb_tmp1 -> vntb_tmp2 ---' AS test;
CREATE VTABLE vntb_tmp1 (ts timestamp, int_col int from org_ntb.int_col);
CREATE VTABLE vntb_tmp2 (ts timestamp, int_col int from vntb_tmp1.int_col);
SELECT COUNT(*) FROM vntb_tmp2;
DROP TABLE vntb_tmp1;
SELECT '--- query vntb_tmp2 after dropping vntb_tmp1 (expect Table does not exist error) ---' AS test;
SELECT * FROM vntb_tmp2;

-- ============================================================
-- 42. Concurrent read: multiple queries on same vstb chain
-- ============================================================

SELECT '=== 42. Multiple queries on vstb2 in sequence ===' AS test;
SELECT COUNT(*) FROM vstb2;
SELECT MIN(int_col) FROM vstb2;
SELECT MAX(int_col) FROM vstb2;
SELECT SUM(int_col) FROM vstb2;
SELECT AVG(int_col) FROM vstb2;
SELECT FIRST(int_col) FROM vstb2;
SELECT LAST(int_col) FROM vstb2;
SELECT SPREAD(int_col) FROM vstb2;

-- ============================================================
-- 43. Single column vtable through chain
-- ============================================================

SELECT '=== 43. Single column vtable chain ===' AS test;
CREATE VTABLE vntb_single (ts timestamp, int_col int from org_ntb.int_col);
CREATE VTABLE vntb_single2 (ts timestamp, int_col int from vntb_single.int_col);
SELECT * FROM vntb_single2 ORDER BY ts;

-- ============================================================
-- 44. information_schema queries
-- ============================================================

SELECT '=== 44. information_schema ===' AS test;
SELECT table_name, type FROM information_schema.ins_tables WHERE db_name = 'test_vtable_ref' AND type = 'VIRTUAL_NORMAL_TABLE' ORDER BY table_name LIMIT 10;
SELECT table_name, type FROM information_schema.ins_tables WHERE db_name = 'test_vtable_ref' AND type = 'VIRTUAL_CHILD_TABLE' ORDER BY table_name LIMIT 10;
SELECT stable_name FROM information_schema.ins_stables WHERE db_name = 'test_vtable_ref' AND stable_name LIKE 'vstb%' ORDER BY stable_name;

-- ============================================================
-- DONE
-- ============================================================
SELECT '=== ALL TESTS COMPLETED ===' AS test;

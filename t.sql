-- =============================================================================
-- t.sql — VST Inheritance 全功能测试
-- 对应 FS: 17-vst-inheritance-fs.md
-- 用法: taos -f t.sql
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. 环境准备
-- ─────────────────────────────────────────────────────────────────────────────
DROP DATABASE IF EXISTS db_vst_inh;
DROP DATABASE IF EXISTS db_vst_cross;
CREATE DATABASE db_vst_inh VGROUPS 2;
CREATE DATABASE db_vst_cross VGROUPS 2;
USE db_vst_inh;

-- 源数据表（供 VCT col-ref / private 列引用）
CREATE STABLE src_stb (ts TIMESTAMP, c1 INT, c2 FLOAT, c3 NCHAR(64)) TAGS (t1 INT);
CREATE TABLE src_t1 USING src_stb TAGS (1);
CREATE TABLE src_t2 USING src_stb TAGS (2);
CREATE TABLE src_t3 USING src_stb TAGS (3);
CREATE TABLE src_t4 USING src_stb TAGS (4);
INSERT INTO src_t1 VALUES ('2026-01-01 00:00:00', 10, 1.1, 'hello');
INSERT INTO src_t2 VALUES ('2026-01-01 00:00:01', 20, 2.2, 'world');
INSERT INTO src_t3 VALUES ('2026-01-01 00:00:02', 30, 3.3, 'foo');
INSERT INTO src_t4 VALUES ('2026-01-01 00:00:03', 40, 4.4, 'bar');

-- 跨库源表
USE db_vst_cross;
CREATE STABLE cross_stb (ts TIMESTAMP, x1 INT, x2 DOUBLE) TAGS (xt1 NCHAR(32));
CREATE TABLE cross_t1 USING cross_stb TAGS ('sensor_a');
CREATE TABLE cross_t2 USING cross_stb TAGS ('sensor_b');
INSERT INTO cross_t1 VALUES ('2026-01-01 00:00:00', 100, 10.1);
INSERT INTO cross_t2 VALUES ('2026-01-01 00:00:01', 200, 20.2);

USE db_vst_inh;

-- =============================================================================
-- 1. DDL: CREATE VIRTUAL STABLE — 基础继承
-- =============================================================================

-- 1.1 创建根 VST
CREATE VIRTUAL STABLE vst_root (
    ts TIMESTAMP,
    val INT REF db_vst_inh.src_stb.c1
) TAGS (
    region NCHAR(64) REF db_vst_inh.src_stb.t1
);

-- 1.2 创建子 VST（继承 vst_root，新增 extra 列）
CREATE VIRTUAL STABLE vst_mid BASE ON vst_root (
    extra INT REF db_vst_inh.src_stb.c1
) TAGS (
    mid_tag NCHAR(32) REF db_vst_inh.src_stb.t1
);

-- 1.3 创建第二个子 VST（继承 vst_root，新增 temp 列）
CREATE VIRTUAL STABLE vst_mid2 BASE ON vst_root (
    temp FLOAT REF db_vst_inh.src_stb.c2
) TAGS (
    mid2_tag INT REF db_vst_inh.src_stb.t1
);

-- 1.4 验证子 VST schema = 父列 + 新增列
DESCRIBE vst_root;
-- 期望: ts, val, region
DESCRIBE vst_mid;
-- 期望: ts, val, extra, region, mid_tag（继承 ts/val/region + 新增 extra/mid_tag）
DESCRIBE vst_mid2;
-- 期望: ts, val, temp, region, mid2_tag

-- =============================================================================
-- 2. DDL: CREATE — 错误场景
-- =============================================================================

-- 2.1 父表不是 VST → 报错
-- expect: error (parent is not a virtual stable)
CREATE VIRTUAL STABLE vst_bad_parent BASE ON src_stb (
    col1 INT REF db_vst_inh.src_stb.c1
) TAGS (
    t1 INT REF db_vst_inh.src_stb.t1
);

-- 2.2 列名与父表冲突 → 报错
-- expect: error (column name conflicts with parent)
CREATE VIRTUAL STABLE vst_col_conflict BASE ON vst_root (
    val INT REF db_vst_inh.src_stb.c1
) TAGS (
    dup_tag INT REF db_vst_inh.src_stb.t1
);

-- 2.3 Tag 名与父表冲突 → 报错
-- expect: error (tag name conflicts with parent)
CREATE VIRTUAL STABLE vst_tag_conflict BASE ON vst_root (
    new_col INT REF db_vst_inh.src_stb.c1
) TAGS (
    region NCHAR(64) REF db_vst_inh.src_stb.t1
);

-- 2.4 深度限制 (max 10): 构建 10 级链条 → 第 11 级报错
CREATE VIRTUAL STABLE vst_d1 BASE ON vst_mid (d1_col INT REF db_vst_inh.src_stb.c1) TAGS (d1_tag INT REF db_vst_inh.src_stb.t1);
-- depth=2 (root=0, mid=1, d1=2)
CREATE VIRTUAL STABLE vst_d2 BASE ON vst_d1 (d2_col INT REF db_vst_inh.src_stb.c1) TAGS (d2_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d3 BASE ON vst_d2 (d3_col INT REF db_vst_inh.src_stb.c1) TAGS (d3_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d4 BASE ON vst_d3 (d4_col INT REF db_vst_inh.src_stb.c1) TAGS (d4_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d5 BASE ON vst_d4 (d5_col INT REF db_vst_inh.src_stb.c1) TAGS (d5_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d6 BASE ON vst_d5 (d6_col INT REF db_vst_inh.src_stb.c1) TAGS (d6_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d7 BASE ON vst_d6 (d7_col INT REF db_vst_inh.src_stb.c1) TAGS (d7_tag INT REF db_vst_inh.src_stb.t1);
CREATE VIRTUAL STABLE vst_d8 BASE ON vst_d7 (d8_col INT REF db_vst_inh.src_stb.c1) TAGS (d8_tag INT REF db_vst_inh.src_stb.t1);
-- depth=9 (d8), now d9 would be depth=10 → still OK
CREATE VIRTUAL STABLE vst_d9 BASE ON vst_d8 (d9_col INT REF db_vst_inh.src_stb.c1) TAGS (d9_tag INT REF db_vst_inh.src_stb.t1);
-- depth=10 → this is the limit, should succeed

-- expect: error (inheritance depth exceeds limit of 10)
CREATE VIRTUAL STABLE vst_d10 BASE ON vst_d9 (d10_col INT REF db_vst_inh.src_stb.c1) TAGS (d10_tag INT REF db_vst_inh.src_stb.t1);

-- 2.5 跨库继承 → 应成功
USE db_vst_cross;
CREATE VIRTUAL STABLE cross_vst_root (
    ts TIMESTAMP,
    x1 INT REF db_vst_cross.cross_stb.x1
) TAGS (
    xt1 NCHAR(32) REF db_vst_cross.cross_stb.xt1
);
USE db_vst_inh;

CREATE VIRTUAL STABLE vst_cross_child BASE ON db_vst_cross.cross_vst_root (
    local_col INT REF db_vst_inh.src_stb.c1
) TAGS (
    local_tag INT REF db_vst_inh.src_stb.t1
);

-- =============================================================================
-- 3. DDL: CREATE VCT — 含私有列
-- =============================================================================

-- 3.1 在 vst_root 下创建 VCT
CREATE VIRTUAL TABLE vct_r1 USING vst_root TAGS ('north')
(
    ts FROM db_vst_inh.src_t1.ts,
    val FROM db_vst_inh.src_t1.c1
);
CREATE VIRTUAL TABLE vct_r2 USING vst_root TAGS ('south')
(
    ts FROM db_vst_inh.src_t2.ts,
    val FROM db_vst_inh.src_t2.c1
);

-- 3.2 在 vst_mid 下创建 VCT，含私有列 sensor_a
CREATE VIRTUAL TABLE vct_m1 USING vst_mid TAGS ('east', 'mid-01')
(
    ts FROM db_vst_inh.src_t3.ts,
    val FROM db_vst_inh.src_t3.c1,
    extra FROM db_vst_inh.src_t3.c1
) PRIVATE (
    sensor_a FROM db_vst_inh.src_t3.c3
);

-- 3.3 在 vst_mid2 下创建 VCT，含私有列 sensor_b
CREATE VIRTUAL TABLE vct_m2 USING vst_mid2 TAGS ('west', 4)
(
    ts FROM db_vst_inh.src_t4.ts,
    val FROM db_vst_inh.src_t4.c1,
    temp FROM db_vst_inh.src_t4.c2
) PRIVATE (
    sensor_b FROM db_vst_inh.src_t4.c3
);

-- =============================================================================
-- 4. DDL: DROP — 继承保护
-- =============================================================================

-- 4.1 DROP 有子 VST 的父 VST → 拒绝
-- expect: error (VST has child virtual stables)
DROP VIRTUAL STABLE vst_root;

-- 4.2 DROP 叶子 VST → 成功
CREATE VIRTUAL STABLE vst_leaf BASE ON vst_mid2 (
    leaf_col INT REF db_vst_inh.src_stb.c1
) TAGS (
    leaf_tag INT REF db_vst_inh.src_stb.t1
);
DROP VIRTUAL STABLE vst_leaf;
-- expect: success

-- 4.3 先删子再删父 → 都成功
CREATE VIRTUAL STABLE vst_tmp_parent (
    ts TIMESTAMP,
    v1 INT REF db_vst_inh.src_stb.c1
) TAGS (
    tag1 INT REF db_vst_inh.src_stb.t1
);
CREATE VIRTUAL STABLE vst_tmp_child BASE ON vst_tmp_parent (
    v2 INT REF db_vst_inh.src_stb.c1
) TAGS (
    tag2 INT REF db_vst_inh.src_stb.t1
);
DROP VIRTUAL STABLE vst_tmp_child;
-- expect: success
DROP VIRTUAL STABLE vst_tmp_parent;
-- expect: success

-- =============================================================================
-- 5. DDL: ALTER CASCADE
-- =============================================================================

-- 5.1 父 ADD COLUMN → 自动级联到所有子孙
ALTER VIRTUAL STABLE vst_root ADD COLUMN new_val BIGINT;
DESCRIBE vst_root;
-- expect: ts, val, new_val, region
DESCRIBE vst_mid;
-- expect: ts, val, new_val, extra, region, mid_tag（new_val 被级联插入继承区域）
DESCRIBE vst_mid2;
-- expect: ts, val, new_val, temp, region, mid2_tag

-- 5.2 父 DROP COLUMN → 拒绝（有子 VST）
-- expect: error (VST has child virtual stables)
ALTER VIRTUAL STABLE vst_root DROP COLUMN new_val;

-- 5.3 父 MODIFY COLUMN → 级联到所有子孙（如果类型兼容变更）
-- 示例：将 NCHAR 列扩大长度
ALTER VIRTUAL STABLE vst_root MODIFY TAG region NCHAR(128);
DESCRIBE vst_mid;
-- expect: region 的长度变为 NCHAR(128)
DESCRIBE vst_mid2;
-- expect: region 的长度变为 NCHAR(128)

-- 5.4 子 VST ADD COLUMN → 仅影响自身，不影响父
ALTER VIRTUAL STABLE vst_mid ADD COLUMN mid_extra DOUBLE;
DESCRIBE vst_mid;
-- expect: ts, val, new_val, extra, mid_extra, region, mid_tag
DESCRIBE vst_root;
-- expect: ts, val, new_val, region（无 mid_extra）

-- =============================================================================
-- 6. SHOW VSTABLE INHERITS
-- =============================================================================

SHOW VSTABLE INHERITS;
-- expect: 至少包含:
--   parent=vst_root, child=vst_mid,  depth=1
--   parent=vst_root, child=vst_mid2, depth=1
--   parent=vst_mid,  child=vst_d1,   depth=2
--   ...

SELECT * FROM information_schema.ins_inherits;
-- 期望与 SHOW VSTABLE INHERITS 结果一致

-- =============================================================================
-- 7. DQL: 基础查询（无 EXPAND）— 向后兼容
-- =============================================================================

-- 7.1 查询 VST → 仅自身 VCT
SELECT * FROM vst_root;
-- expect: 仅 vct_r1, vct_r2 的数据
-- 列: ts, val, new_val
-- vct_r1: ts='2026-01-01 00:00:00', val=10
-- vct_r2: ts='2026-01-01 00:00:01', val=20

-- 7.2 查询子 VST → 仅其自身 VCT
SELECT * FROM vst_mid;
-- expect: 仅 vct_m1
-- 列: ts, val, new_val, extra, mid_extra

-- 7.3 VCT 直接查询 → 基础列 + 私有列
SELECT * FROM vct_m1;
-- expect: ts, val, new_val, extra, mid_extra, sensor_a
-- sensor_a 是私有列，只在 VCT 直接查询时可见

SELECT * FROM vct_m2;
-- expect: ts, val, new_val, temp, sensor_b

-- =============================================================================
-- 8. DQL: EXPAND 语法
-- =============================================================================

-- 8.1 EXPAND（无参数）→ 等同 EXPAND(0)，不展开
SELECT * FROM vst_root EXPAND;
-- expect: 同 SELECT * FROM vst_root（仅 vct_r1, vct_r2）

-- 8.2 EXPAND(0) → 不展开
SELECT * FROM vst_root EXPAND(0);
-- expect: 同上

-- 8.3 EXPAND(1) → 展开 1 层子孙
SELECT * FROM vst_root EXPAND(1);
-- expect: vct_r1, vct_r2, vct_m1, vct_m2
-- 列并集: ts, val, new_val, extra, temp（不含私有列 sensor_a/sensor_b）
-- vct_r1: ts=00:00, val=10, new_val=NULL, extra=NULL, temp=NULL
-- vct_r2: ts=00:01, val=20, new_val=NULL, extra=NULL, temp=NULL
-- vct_m1: ts=00:02, val=30, new_val=NULL, extra=30,   temp=NULL
-- vct_m2: ts=00:03, val=40, new_val=NULL, extra=NULL,  temp=4.4

-- 8.4 EXPAND(-1) → 递归展开全部子孙
SELECT * FROM vst_root EXPAND(-1);
-- expect: 与 EXPAND(1) 相同（本例只有 1 层子 VST 有 VCT）

-- 8.5 从子 VST 开始 EXPAND
SELECT * FROM vst_mid EXPAND(-1);
-- expect: vct_m1 + vst_d1~d9 的 VCT（如果有的话）
-- 本例 vst_d1~d9 下无 VCT，所以仅 vct_m1

-- =============================================================================
-- 9. DQL: EXPAND — 列可见性与 NULL 填充
-- =============================================================================

-- 9.1 列并集不含 VCT 私有列
SELECT * FROM vst_root EXPAND(-1);
-- 列: ts, val, new_val, extra, mid_extra, temp（不含 sensor_a, sensor_b）
-- vct_r1: extra=NULL, temp=NULL
-- vct_m1: extra=30,   temp=NULL
-- vct_m2: extra=NULL,  temp=4.4

-- 9.2 VCT 直接查询仍包含私有列
SELECT * FROM vct_m1;
-- 列应包含 sensor_a

-- =============================================================================
-- 10. DQL: EXPAND — 过滤与聚合
-- =============================================================================

-- 10.1 tbname 过滤
SELECT * FROM vst_root EXPAND(-1) WHERE tbname = 'vct_m1';
-- expect: 仅 vct_m1 的数据

-- 10.2 聚合
SELECT COUNT(*) FROM vst_root EXPAND(-1);
-- expect: 4（vct_r1 + vct_r2 + vct_m1 + vct_m2 各 1 行）

SELECT COUNT(*) FROM vst_root;
-- expect: 2（仅 vct_r1 + vct_r2）

-- 10.3 按 tbname 分组
SELECT tbname, COUNT(*) FROM vst_root EXPAND(-1) GROUP BY tbname ORDER BY tbname;
-- expect:
--   vct_m1  1
--   vct_m2  1
--   vct_r1  1
--   vct_r2  1

-- 10.4 带列条件过滤
SELECT * FROM vst_root EXPAND(-1) WHERE val > 15;
-- expect: vct_r2(val=20), vct_m1(val=30), vct_m2(val=40)

-- 10.5 只查询继承列（父 VST 定义的列）
SELECT ts, val FROM vst_root EXPAND(-1) ORDER BY ts;
-- expect: 4 行，按 ts 排序

-- =============================================================================
-- 11. DQL: EXPAND — 错误场景
-- =============================================================================

-- 11.1 对非继承 VST 使用 EXPAND(N>0) → 报错
CREATE VIRTUAL STABLE vst_standalone (
    ts TIMESTAMP,
    v1 INT REF db_vst_inh.src_stb.c1
) TAGS (
    t1 INT REF db_vst_inh.src_stb.t1
);
-- expect: error (EXPAND used on non-inherited VST)
SELECT * FROM vst_standalone EXPAND(1);

-- 11.2 INSERT/DELETE 不支持 EXPAND → 语法错误
-- expect: syntax error
-- INSERT INTO vst_root EXPAND(-1) VALUES (...);
-- DELETE FROM vst_root EXPAND(-1) WHERE ts < '2026-01-02';

-- 11.3 EXPAND(0) 对非继承 VST → 应成功（等同不展开）
SELECT * FROM vst_standalone EXPAND(0);
-- expect: success, 返回 vst_standalone 自身 VCT

-- =============================================================================
-- 12. DCL: 权限继承
-- =============================================================================

-- 12.1 创建测试用户
CREATE USER test_user PASS 'Test123!';

-- 12.2 授权父 VST
GRANT READ ON db_vst_inh.vst_root TO test_user;

-- 12.3 创建子 VST → 应自动继承父 VST 权限
CREATE VIRTUAL STABLE vst_perm_child BASE ON vst_root (
    perm_col INT REF db_vst_inh.src_stb.c1
) TAGS (
    perm_tag INT REF db_vst_inh.src_stb.t1
);
-- expect: test_user 自动拥有 vst_perm_child 的 READ 权限

-- 12.4 子 VST 追加额外权限
GRANT WRITE ON db_vst_inh.vst_perm_child TO test_user;
-- expect: test_user 对 vst_perm_child 拥有 READ + WRITE

-- 12.5 父 VST 权限变更 → 覆盖子孙权限
REVOKE READ ON db_vst_inh.vst_root FROM test_user;
-- expect: test_user 对 vst_root 和 vst_perm_child 的 READ 均被撤销
-- (vst_perm_child 的 WRITE 保留与否取决于 FS 定义的"覆盖"语义)

-- 12.6 验证权限状态
SHOW GRANTS;

-- 清理
DROP VIRTUAL STABLE vst_perm_child;
DROP USER test_user;

-- =============================================================================
-- 13. 多层继承 EXPAND 测试
-- =============================================================================

-- 构建 3 层继承: root → mid → leaf_vst
CREATE VIRTUAL STABLE vst_leaf_vst BASE ON vst_mid (
    leaf_val DOUBLE REF db_vst_inh.src_stb.c2
) TAGS (
    leaf_tag NCHAR(16) REF db_vst_inh.src_stb.t1
);

-- 在 leaf_vst 下创建 VCT
CREATE VIRTUAL TABLE vct_leaf1 USING vst_leaf_vst TAGS ('up', 'mid-02', 'leaf-01')
(
    ts FROM db_vst_inh.src_t1.ts,
    val FROM db_vst_inh.src_t1.c1,
    extra FROM db_vst_inh.src_t1.c1,
    leaf_val FROM db_vst_inh.src_t1.c2
);

-- 13.1 从 root EXPAND(1) → 只含 1 层（mid, mid2），不含 leaf_vst 的 VCT
SELECT COUNT(*) FROM vst_root EXPAND(1);
-- expect: 4（vct_r1 + vct_r2 + vct_m1 + vct_m2）

-- 13.2 从 root EXPAND(2) → 含 2 层，包含 leaf_vst 的 VCT
SELECT COUNT(*) FROM vst_root EXPAND(2);
-- expect: 5（+ vct_leaf1）

-- 13.3 从 root EXPAND(-1) → 全部
SELECT COUNT(*) FROM vst_root EXPAND(-1);
-- expect: 5

-- 13.4 从 mid EXPAND(1) → 含 leaf_vst 的 VCT
SELECT COUNT(*) FROM vst_mid EXPAND(1);
-- expect: 2（vct_m1 + vct_leaf1）

-- 13.5 从 mid EXPAND(-1) → 同上（leaf_vst 下无更深子 VST）
SELECT COUNT(*) FROM vst_mid EXPAND(-1);
-- expect: 2

-- 13.6 列并集验证
SELECT * FROM vst_root EXPAND(-1) ORDER BY ts;
-- 列并集: ts, val, new_val, extra, mid_extra, temp, leaf_val
-- vct_r1:    val=10, extra=NULL, temp=NULL, leaf_val=NULL
-- vct_r2:    val=20, extra=NULL, temp=NULL, leaf_val=NULL
-- vct_m1:    val=30, extra=30,   temp=NULL, leaf_val=NULL
-- vct_m2:    val=40, extra=NULL, temp=4.4,  leaf_val=NULL
-- vct_leaf1: val=10, extra=10,   temp=NULL, leaf_val=1.1

-- =============================================================================
-- 14. 边界场景
-- =============================================================================

-- 14.1 IF NOT EXISTS
CREATE VIRTUAL STABLE IF NOT EXISTS vst_mid BASE ON vst_root (
    extra INT REF db_vst_inh.src_stb.c1
) TAGS (
    mid_tag NCHAR(32) REF db_vst_inh.src_stb.t1
);
-- expect: success (ignored, already exists)

-- 14.2 空子表的 EXPAND
CREATE VIRTUAL STABLE vst_empty_child BASE ON vst_root (
    empty_col INT REF db_vst_inh.src_stb.c1
) TAGS (
    empty_tag INT REF db_vst_inh.src_stb.t1
);
-- vst_empty_child 下无 VCT
SELECT COUNT(*) FROM vst_root EXPAND(-1);
-- expect: 5（不变，empty_child 无 VCT 不影响结果）

-- 14.3 DESCRIBE 继承后的子 VST（验证 schema 完整）
DESCRIBE vst_leaf_vst;
-- expect: ts, val, new_val, extra, mid_extra, leaf_val, region, mid_tag, leaf_tag
-- 即 root 列 + mid 列 + leaf 自身列

-- 14.4 系统表查继承链
SELECT * FROM information_schema.ins_inherits WHERE child_stable LIKE '%leaf%';
-- expect: parent=vst_mid, child=vst_leaf_vst, depth=2

-- =============================================================================
-- 15. 清理
-- =============================================================================

-- 按从叶到根顺序清理深层继承链
DROP VIRTUAL STABLE vst_d9;
DROP VIRTUAL STABLE vst_d8;
DROP VIRTUAL STABLE vst_d7;
DROP VIRTUAL STABLE vst_d6;
DROP VIRTUAL STABLE vst_d5;
DROP VIRTUAL STABLE vst_d4;
DROP VIRTUAL STABLE vst_d3;
DROP VIRTUAL STABLE vst_d2;
DROP VIRTUAL STABLE vst_d1;

DROP VIRTUAL STABLE vst_empty_child;
DROP VIRTUAL TABLE vct_leaf1;
DROP VIRTUAL STABLE vst_leaf_vst;
DROP VIRTUAL TABLE vct_m1;
DROP VIRTUAL TABLE vct_m2;
DROP VIRTUAL STABLE vst_mid;
DROP VIRTUAL STABLE vst_mid2;
DROP VIRTUAL TABLE vct_r1;
DROP VIRTUAL TABLE vct_r2;
DROP VIRTUAL STABLE vst_root;

DROP VIRTUAL STABLE vst_standalone;
DROP VIRTUAL STABLE vst_cross_child;

DROP DATABASE db_vst_inh;
DROP DATABASE db_vst_cross;

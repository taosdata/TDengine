# taosdump 支持虚拟表 导入/导出 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 | YYYY-MM-DD | 0.1 | 裴亚明 | 初始版本 |

## 2. 测试目标

- 验证 taosdump 对虚拟表的导出/导入正确性。
- 验证“实体优先、虚拟随后”的双阶段导入流程（physical-> virtual）。
- 验证 Avro 元数据新增字段 sql（携带建表语句）读写正确且向后兼容。

## 3. 参考文档

[taosdump 支持虚拟表 导入/导出 - FS](https://taosdata.feishu.cn/wiki/EdS2w6oTXiszDRkBPVLcIY0qn5c)

## 4. 测试结论

测试通过，功能符合预期

## 5. 测试环境

| 客户端 | 192.168.1.54 |
| --- | --- |
| 服务端 | 192.168.1.43 |
| 操作系统 | Ubuntu 20.04.6 LTS (64-bit) |
| CPU和内存 | 40C 251G |
| 存储 | 447G SSD * 2、1.76T SSD |
| TDengine 版本 | TDengine TSDB-Enterprise taosd version: 3.4.0.1.enterprise compatible_version: 3.0.0.0 git: 46fe6ecfe006e342dbe45f4e98fa3f63c33dd78b gitOfInternal: 69cc321b3e217748eec2cfdda238f5b6b9d25ee0 build: Linux-x64 2026-01-26 23:59:16 +0800 |

## 6. 功能测试

### 6.1 导出功能

#### 6.1.1 测试要点

- 数据库包含 虚拟超级表(VST)、虚拟普通表(VTABLE)、实体超级表、子表、普通表的混合导出。
- schemaonly 模式：虚拟表、实体表仅导出 schema；数据文件不应包含虚拟表数据。
- 非 schemaonly 模式：实体表导出数据；虚拟表仍只导出 schema。
- Avro 元数据包含 sql 字段（nullable string），虚拟对象必须写入 Create SQL。
- 指定对象导出（数据库 / 单个 VST / 单个 VT 表）。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 导出仅 schema（含虚拟与实体） | 步骤: taosdump -D idmp -o ./tmp -s 期望: 生成 dbs.sql；虚拟/实体均写入建表；无 .avro 数据；avro-tbtags/avro-ntb 中 sql 字段存在（虚拟有值、实体为 null） | 符合预期 |
| 2 | 导出 schema+数据（混合库） | 步骤: taosdump -D idmp -o ./tmp 期望: 实体表生成 .avro 数据文件；虚拟表无数据文件；dbs.sql/avro 元数据完整 | 符合预期 |
| 3 | 导出指定虚拟超级表 | 步骤: taosdump -o ./out_spec idmp vst_Dp_01w_300887 期望: 仅输出该 VST 的 schema（含 sql 字段），无数据 | 符合预期 |
| 4 | 导出包含虚拟普通表的库 | 步骤: taosdump -D test -o ./tmp_ntb 期望: avro-ntb 文件 sql 字段为 CREATE VTABLE... 且非空；无数据文件 | 符合预期 |

### 6.2 导入功能

#### 6.2.1 测试要点

- 两次导入流程：physical pass（实体）先，virtual pass（虚拟）后；日志中可见 pass 区分与统计。
- db 重命名 -W 生效（CREATE VTABLE 引用源库名被正确改写）
- avro 读取 sql 字段：VST/VT 需按 virtual pass 执行；实体 pass 跳过 sql 分支。
- 目录遍历：支持根目录与 dump.* 子目录。
- 并发导入数据；Stmt 批量绑定正确。
- 可重入与幂等（已存在对象 IF NOT EXISTS 创建，不报错）。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 基础导入（双阶段） | 步骤: taosdump -i ./tmp 期望: 日志显示 physical pass 完成后再 virtual pass；实体表/子表数据量与源一致；VST/VT 存在且可查询 | 符合预期 |
| 2 | 跨库重命名 | 步骤: 1.taosdump -W "idmp=nidmp" -i ./tmp; 2.taosdump -W "idmp=Ab-_1@#$%^&*()_+1" -i ./tmp -g -e; 期望: 目标库 nidmp 创建；所有 VTABLE 内引用的源库名被正确改写 | 符合预期 |
| 3 | 指定对象导入 | 数据源: ./out_spec（仅某 VST/VT） 步骤: taosdump -i ./out_spec 期望: 仅该 VST/VT 创建成功；无数据导入 | 符合预期 |
| 4 | 幂等性 | 步骤: 连续执行两次 -i 同一路径 期望: 第二次无失败；对象均存在 | 符合预期 |

校验项
- show create table 对比
  - VST/VT 的 show create 与 avro/sql 中记录一致（忽略空白/IF NOT EXISTS）
- 数据正确性（实体表）
  - 行数、时间范围一致；抽检多列值一致
- VTABLE 结果正确性
  - 选择 VTABLE（如 SELECT ... LIMIT 10）有数据且逻辑正确（来自引用源表的拼接/对齐）
- 文件完整性
  - dbs.sql、*.avro-tbtags、*.avro-ntb、.m 文件齐备；无空文件

## 7. 性能测试

1. 构造 idmp 数据库下，创建 11个虚拟超级表，每个虚拟超级表包含 1万子表，每个子表包含1万条记录，共计 11 万个虚拟子表，测试该数据库的导出/导入性能。
  ```sql
  taos> show vtables;
  Query OK, 110000 row(s) in set (1.396947s)
  ```

  - 导出时长：00:02:35，运行流畅无卡顿，导出目录文件齐全
  - 导入时长：00:00:24，运行流畅无卡顿，导入虚拟表数量和虚拟表创建语句正确

1. 构造 idmp 数据库下，创建超级表 zhenhua_bearing_works，包含 100 万子表，创建 11 各虚拟超级表，400个虚拟子表，测试该数据库在创建虚拟表前后的导出/导入性能。
  - 创建虚拟表前：
    - 导出时长：
    - 导入时长：
  - 创建虚拟表后：
    - 导出时长：
    - 导入时长：

## 8. 安全测试

- 日志与导出文件不包含敏感凭据。

## 9. 兼容性测试

升级后可导入旧版本导出的数据

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
|  | 旧版本 dump 兼容 | 步骤: 使用老版本 taosdump 导出的数据（无 sql 字段），新版本导入 期望: 实体表导入正常；虚拟对象缺失时跳过 | 符合预期 |

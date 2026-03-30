# taosdump 支持虚拟表 导入/导出 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 | YYYY-MM-DD | 0.1 | @裴亚明 | 初版 |

## 2. 背景

上海电气项目中，交付人员使用 taosdump 导出包含虚拟表的数据库时失败，目前 taosdump 不支持虚拟表的导出/导入，需要开发特性以支持 导出/导入虚拟表。

## 3. 定义

**虚拟表（Virtual Table）**：虚拟表是一种不存储实际数据而可以用于分析计算的表，数据来源为其它真实存储数据的子表、普通表，通过将各个原始表的不同列的数据按照时间戳排序、对齐、合并的方式来生成虚拟表。虚拟表是一种动态数据结构，主要用于解决时间序列数据的多表关联、对齐和整合问题。

## 4. 行为说明

### 4.1 导出

导出数据表时，需要同时导出数据库中的实体表和虚拟表的建模信息，复用超级表和子表的流程。
1. 虚拟超级表导出
  以idmp数据库下虚拟超级表 idmp.`vst_Dp_01w_300887` 为例：
   - 查询该虚拟超级表的字段定义信息
  ```yaml
  DESCRIBE idmp.`vst_Dp_01w_300887`
  ```

   - 查看虚拟超级表的建表语句
  ```yaml
  show create table `idmp`.`vst_Dp_01w_300887`
  ```

将建表语句写入到数据库下文件 dbs.sql 中，与超级表导出流程保持一致。
1. 虚拟子表导出
将建表语句保存在 avro-tbtags 文件中，与子表导出流程保持一致。
1. 虚拟普通表导出
建表语句保存在 avro-ntb 文件中，与普通表导出流程保持一致。

### 4.2 导入

导入数据数据表时，需要区分实体表和虚拟表，因为虚拟表依赖实体表，因此为虚拟表建模时，需要确保依赖的实体表已经存在。基于此实现方案：
程序支持跨数据库域的依赖关系识别或划分，程序保证全局中优先处理实体表建模，然后再处理虚拟表建模。
- 读取数据库下 dbs.sql 实现虚拟超级表的创建。
- 读取数据库下 avro 文件中建表语句实现虚拟子表的创建。

## 5. 性能

导出导入虚拟表在常规时间内完成，与现有超级表、子表导入/导出性能一致。

## 6. 安全

不涉及

## 7. 兼容性

1. 新发布版本应支持原有命令行参数
2. 低版本 taosdump 导出的数据，高版本 taosdump 能够导入

## 8. 运维

无

## 9. 使用场景

以上海电气项目中中导出 idmp 数据库中虚拟表为例：
1. 创建实体数据库和超级表
```sql
CREATE DATABASE `machine_tool_vibration_us` BUFFER 32 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'us' REPLICA 1 WAL_LEVEL 1 VGROUPS 4 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h;
use machine_tool_vibration_us;

CREATE STABLE `no1_machine_tool_works` (
  `ts` TIMESTAMP,
  `1x` DOUBLE,
  `1y` DOUBLE,
  `1z` DOUBLE,
  `2x` DOUBLE,
  `2y` DOUBLE,
  `2z` DOUBLE,
  `3x` DOUBLE,
  `3y` DOUBLE,
  `3z` DOUBLE,
  `4x` DOUBLE,
  `4y` DOUBLE,
  `4z` DOUBLE,
  `5x` DOUBLE,
  `5y` DOUBLE,
  `5z` DOUBLE
) TAGS (`name_id` VARCHAR(16));
```


1. 使用 taosgen 生成数据
配置文件：data.ymal
```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/machine_tool_vibration_us
  drop_if_exists: false
schema:
  name: no1_machine_tool_works
  tbname:
    prefix: dp_5_virbration_0
    count: 2
    from: 1
  columns:
    - name: ts
      type: timestamp
      start: 2026-01-14 14:42:50.299
      precision: us
      step: 1
    - name: 1x
      type: double
      min: -1
      max: 1
    - name: 1y
      type: double
      min: -1
      max: 1
    - name: 1z
      type: double
      min: -1
      max: 1
    - name: 2x
      type: double
      min: -1
      max: 1
    - name: 2y
      type: double
      min: -1
      max: 1
    - name: 2z
      type: double
      min: -1
      max: 1
    - name: 3x
      type: double
      min: -1
      max: 1
    - name: 3y
      type: double
      min: -1
      max: 1
    - name: 3z
      type: double
      min: -1
      max: 1
    - name: 4x
      type: double
      min: -1
      max: 1
    - name: 4y
      type: double
      min: -1
      max: 1
    - name: 4z
      type: double
      min: -1
      max: 1
    - name: 5x
      type: double
      min: -1
      max: 1
    - name: 5y
      type: double
      min: -1
      max: 1
    - name: 5z
      type: double
      min: -1
      max: 1
  tags:
    - name: name_id
      type: varchar(16)
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 10000
    num_cached_batches: 0
jobs:
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 1
      - uses: tdengine/insert
        with:
          concurrency: 1
```


运行并生成数据：
```bash
taosgen -c data.yaml
```


1. 创建虚拟超级表和虚拟子表
```bash
CREATE DATABASE `idmp` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'us' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h;
use idmp;
CREATE STABLE `vst_Dp_04_137845` (
  `ts` TIMESTAMP,
  `主轴X方向上的振动` DOUBLE,
  `主轴Y方向上的振动` DOUBLE,
  `主轴Z方向上的振动` DOUBLE,
  `立柱X方向上的振动` DOUBLE,
  `立柱Y方向上的振动` DOUBLE,
  `立柱Z方向上的振动` DOUBLE,
  `地面X方向上的振动` DOUBLE,
  `地面Y方向上的振动` DOUBLE,
  `地面Z方向上的振动` DOUBLE
) TAGS (`element` VARCHAR(256), `path1` VARCHAR(512)) VIRTUAL 1;

CREATE VTABLE `vt_Dp_04_969098` (
  `主轴X方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`1x`,
  `主轴Y方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`1y`,
  `主轴Z方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`1z`,
  `立柱X方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`3x`,
  `立柱Y方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`3y`,
  `立柱Z方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`3z`,
  `地面X方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`2x`,
  `地面Y方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`2y`,
  `地面Z方向上的振动` FROM `machine_tool_vibration_us`.`dp_3_virbration_04`.`2z`
) USING `vst_Dp_04_137845` (`element`, `path1`) TAGS ("Dp_04", "AI驱动高端装备健康分析场景.机床.一机床厂");
```


1. 导出并导入包含虚拟表的数据库 idmp
```bash

## 10. 导出：

taosdump -D idmp -o ./tmp -s

## 11. 导入：

taosdump -W "idmp=vidmp" -i ./tmp
```


## 12. 约束和限制

约束：无
限制：无

## 13. 常见错误和排查

在开发调试过程中补充。

## 14. 可观测性

用户交互界面中输出当前导出/导入信息。

## 15. 安装和卸载

跟随 TDengine TSDB Server/Client 安装或卸载

## 16. 文档

需要修改官网文档

## 17. 参考文档

## 18. 附录

# 优化 compact 使用体验

## 1. 背景

背景见以下相关资料：
1. 
  TS-4994

1. 
  TD-30555

1. [TDengine 可运维观测需求](https://taosdata.feishu.cn/wiki/OrX7woLVbiGy0ekld25c141VnPf) 第 8、9、10、11 项
2. [需求说明：优化 compact 使用体验](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/08/02 | 0.1 | 程洪泽、徐开礼 | 初稿 |
| 2024/08/05 | 0.2 | 程洪泽、徐开礼 | 根据线上和线下 Review 意见修改 |

## 3. 定义 {folded="true"}

无

## 4. 行为说明

### 4.1 自动 COMPACT

#### 4.1.1 创建数据库时指定相关参数

```sql {wrap}
CREATE DATABASE db [COMPACT_INTERVAL '10d'] [COMPACT_TIME_RANGE '-60d,-30d'][COMPACT_TIME_OFFSET '8h'];
```

为满足[需求 1](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac#Jfjodyc5zozwTyxijRgc1ezvn7f) 中描述的通过任务自动触发 compact 的需求，为数据库增加 COMPACT 相关参数，以控制自动 compact：
1. COMPACT_INTERVAL：自动 compact 的触发周期，如 SQL 示例中，数据库 db 的 compact 触发周期是 10 天(从 1970-01-01T00:00:00Z 开始切分的时间周期)。取值范围：0 或 [10m, keep2]，单位：m(分钟),h(小时),d(天)。**不加时间单位****默认单位为天**，**默认值为 0，即不触发自动 compact 功能**。mnode 根据 db 的 compact 配置信息定期下发 compact 任务；如果 db 中有未完成的 compact 任务，不重复下发 compact 任务。**注：内部测试中可以使用各种单位，但文档上说明只支持以天为单位**。
2. COMPACT_TIME_RANGE：自动 compact 任务可触发 COMPACT 的时间范围，如 SQL 示例中，超过 30 天但小于 60 天的数据在自动 compact 任务触发时会进行扫描并决定是否进行 compact。对于数据在 30 天以内或 60 天以上的文件组，自动 compact 不对这些文件组做扫描或操作。取值范围：[-keep2, -duration]，单位：m(分钟),h(小时),d(天)。不加时间单位时默认单位为天，默认值为 [0, 0]（注：在默认值 [0, 0] 时，如果 COMPACT_INTERVAL 大于 0，也会按照 [-keep2, -duration] 下发自动 compact。原因：用户想按照整个保存周期下发自动 compact，如果最初设置了 [-keep2, -duration]，后期又修改了 keep2，有可能会忘记修改 COMPACT_TIME_RANGE 中的 -keep2。因此，要关闭自动 compact 功能，需要将 COMPACT_INTERVAL 设置为 0。）
3. COMPACT_TIME_OFFSET：自动 compact 任务可触发 COMPACT 的相对本地时间的偏移量。取值范围：[0,23]，单位: h(小时），默认值为 0。

#### 4.1.2 通过 alter 命令修改相关参数

```sql {wrap}
ALTER DATABASE db COMPACT_INTERVAL '10d' COMPACT_TIME_RANGE '-90,-30d' COMPACT_TIME_OFFSET '1h';
```

为满足[需求 3](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac#Jfjodyc5zozwTyxijRgc1ezvn7f)，允许对数据库的 COMPACT 相关参数进行修改，修改时的约束与建库时的约束相同。具体参数意义及取值见 [4.1.1](https://taosdata.feishu.cn/docx/V2m9d8vnyotJmfx2YPacsC13nmd#XMLgd0F5yoXSHkxtLHKcUKaxnGf) 。

### 4.2 对指定 VGROUP 列表做 COMPACT

为满足[需求 1](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac#Jfjodyc5zozwTyxijRgc1ezvn7f) 中描述的支持指定 VGROUP ID 下发 COMPACT 任务，新增如下 SQL 语句：
```sql
COMPACT [dbname.]vgroups in (2, 3, 4) [start_opt] [end_opt]; 
```

如果指定的列表中的 VNODE 不属于同一个 DB 或有不存在的 VNODE 则报错处理，命令执行失败。start_opt 和 end_opt 与 compact db 的语义及行为一致。

### 4.3 查看 VNODE 中文件组状态及相关信息

为满足[需求 4](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac#WSxFdOQb2ooXxHxmv7kceEk4nec)，增加以下查询语句支持功能，用于查询各 VNODE 上各文件组的相关信息：
```sql
select * from information_schema.ins_fsets;
```

其输出内容及格式如下：
```cpp
  dnode_id ｜ db_name | vgroup_id | fset_id |       start_time        |       end_time          | last_compact_time       | compact_advice | details|  
=====================================================================================================================================================
         1 |    db1   |     2    | 1860     | 2024-07-20 00:00:00.000 | 2024-07-29 23:59:59.999 | 2024-07-30 00:30:00.000 |  no            | ...    |
```

- dnode_id:  DNODE ID
- db_name: 数据库名称
- vgroup_id: VGROUP ID
- fset_id: 文件组的 ID 号
- start_time: 文件组数据覆盖的最小时间
- end_time: 文件组数据覆盖的最大时间
- last_compact_time：上次 compact 的时间
- compact_advice：compact 建议（YES/NO)
   - 文件组中存在多个 STT 文件时，建议 compact
   - 文件组中存在删除记录时，建议 compact
   - data 文件中 空洞数据块占总文件大小的大于等于 30% 时建议 compact
   - 注：
      - 无法对 STT 中数据块进行扫描，判断是否需要 compact。
      - 即使 compact_advice 为 No， 依然可以手动执行 compact，但如果手动执行时扫描文件后判定为不需要 compact，会省略真正的 compact 步骤直接返回。

### 4.4 控制 COMPACT 任务并发数的配置

为控制 COMPACT 任务的并发度，在 taos.cfg 中增加配置参数：
```markdown
numOfCompactThreads 2  # 不要求各个 dnode 间一致
```

COMPACT 任务发送到 VNODE 后会按任务覆盖的时间范围，按文件组拆分成多个子任务，此参数即控制子任务的并发度。如上所示例子中，指定 COMPACT 的最大并发度为 2，即整个 DNODE 允许同时对两个文件组进行 COMPACT 操作。
参数取值范围 [1, 16]，默认为 2。
```sql {wrap}
alter dnode `id` 'numOfCompactThreads' '4';
```

允许用户通过上述命令对参数进行修改，取值同上。

### 4.5 show compact 输出按文件大小的百分比及预估完成时间

增加完成百分比及预估完成时间列：
Progress(%)：完成百分比。
Remaining time(m): 预估剩余分钟数。

## 5. 性能

- 在补录数据的场景中，由于可能对同一个文件组进行操作，自动 compact 功能可能阻塞写入功能，以避免两个并行任务同时修改文件组导致文件损坏问题。
- 自动 COMPACT 任务在触发时可能占用较多资源，影响服务端的写入和查询性能。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

1. 用户数据乱序或更新较多，可使用自动 compact 功能自动 compact
2. 运维交付可利用查询功能查看数据文件大小等

## 9. 约束和限制

约束：仅企业版支持该功能

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档 {folded="true"}

需要修改企业版文档。

## 14. 参考文档 {folded="true"}

1. [TDengine 可运维观测需求](https://taosdata.feishu.cn/wiki/OrX7woLVbiGy0ekld25c141VnPf)
2. [需求说明：优化 compact 使用体验](https://taosdata.feishu.cn/wiki/InNDwAf9YiLroJkMvfWcp1A8nac)

## 15. 附录 {folded="true"}

无

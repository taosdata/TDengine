# 支持敏感数据删除后的强制覆盖 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-24 | 2026-02-28 | 1.0 | 鲍之骁 | 初稿 |

## 2. 背景

**背景：**根据数据安全的要求，用户删除敏感数据后必须保证物理残留不可恢复。当前 TDengine 的 DELETE 仅进行逻辑标记，物理存储位置仍可能残留原始数据，存在敏感数据删除后被恢复的风险。
**目标：**在不影响现有高吞吐特性的前提下，提供删除即强制覆盖 + 定期清理的完整安全删除方案，满足数据安全中对于敏感数据删除的要求，避免敏感数据删除后又被恢复甚至泄漏的风险。

## 3. 定义

1. **Secure Delete（安全删除）：**删除敏感数据时，强制用无用信息（随机数据或零值）覆盖原物理存储位置的功能。
2. **Compaction（数据重组织）：**TDengine 中合并数据文件、移除已删除数据的过程。
3. **TRIM：**TDengine 清除过期数据的命令。

## 4. 行为说明

### 4.1 配置参数

##### 4.1.0.1 secure_erase_mode

- 说明：采用0值或随机数据进行敏感数据删除后的覆盖
- 类型：整数；0：使用 0 值覆盖；1：使用随机数据覆盖。
- 默认值：0
- 最小值：0
- 最大值：1
- 参数类型：全局配置参数
- 动态修改：支持通过 SQL  修改，重启生效
- 支持版本：v3.4.1.0 引入

### 4.2 数据库与超级表支持安全删除选项

开启安全删除选项后，对于数据的删除会对原始数据根据规则进行覆盖。

#### 4.2.1 创建时开启安全删除选项

- `secure_delete` 如果不指定，则默认关闭安全删除功能。
- 超级表的 `secure_delete` 优先级高于数据库。
```sql
-- 开启数据库级安全删除（所有子表默认安全删除）
create database if not exists db vgroups 10 buffer 10 secure_delete 1;

-- 开启超级表级别的安全删除（仅该 STABLE 及其子表生效，优先级高于数据库）
create stable stb(ts timestamp,val int) tags (id int) secure_delete 1;
```

#### 4.2.2 动态修改数据库与超级表的安全选项

```sql
-- 开启数据库级安全删除（所有子表默认安全删除）
ALTER DATABASE power SET secure_delete = 1;
-- 开启超级表级安全删除（仅该 STABLE 及其子表生效，优先级高于数据库）
ALTER STABLE meters SET secure_delete = 1;

-- 关闭
ALTER DATABASE power SET secure_delete = 0;
ALTER STABLE meters SET secure_delete = 0;
```

### 4.3 DELETE 语句新增 SECURE_DELETE 选项

- 当表/库 `secure_delete = 1` 时，普通 `DELETE` 自动进行覆盖。
- `secure_delete` 关键字强制本次删除覆盖，即使表/库选项关闭。
- 覆盖动作在存储引擎层立即执行，保证返回结果前原位置被覆盖。
```sql
-- 普通删除（若表/库 secure_delete=1，则自动覆盖）
DELETE FROM meters WHERE ts < '2025-01-02 00:00:00';
-- 强制安全删除（无论表/库选项如何，强制覆盖）
DELETE FROM meters WHERE ts BETWEEN '2025-01-01 08:00:00' AND '2025-01-01 09:00:00' SECURE_DELETE;
```

### 4.4 定期清理删除数据（已经支持）

安全删除为同步覆盖敏感数据，必定会影响数据库的吞吐量。为了避免数据库性能下降，对于一些敏感程度较低的数据删除，我们可以采取定期批量删除的方式来防止删除数据被恢复。
TDengine 企业版已经支持定期 `compact` 功能 ， `compact` 时会擦除已经被删除的数据。
<quote-container>
**创建数据库时指定：**
**COMPACT_INTERVAL**
自动 compact 触发周期（从 1970-01-01T00:00:00Z 开始切分的时间周期）（**仅企业版支持**）。
- 取值范围：0 或 [10m, keep2]，单位：m（分钟），h（小时），d（天）；
- 不加时间单位默认单位为天，默认值为 0，即不触发自动 compact 功能；
- 如果 db 中有未完成的 compact 任务，不重复下发 compact 任务。
**COMPACT_TIME_RANGE**
自动 compact 任务触发的 compact 时间范围（**仅企业版支持**）。
- 取值范围：[-keep2, -duration]，单位：m（分钟），h（小时），d（天）；
- 不加时间单位时默认单位为天，默认值为 [0, 0]；
- 取默认值 [0, 0] 时，如果 COMPACT_INTERVAL 大于 0，会按照 [-keep2, -duration] 下发自动 compact；
- 因此，要关闭自动 compact 功能，需要将 COMPACT_INTERVAL 设置为 0。
**COMPACT_TIME_OFFSET**
自动 compact 任务触发的 compact 时间相对本地时间的偏移量（**仅企业版支持**）。取值范围：[0, 23]，单位：h（小时），默认值为 0。以 UTC 0 时区为例：
- 如果 COMPACT_INTERVAL 为 1d，当 COMPACT_TIME_OFFSET 为 0 时，在每天 0 点下发自动 compact；
- 如果 COMPACT_TIME_OFFSET 为 2，在每天 2 点下发自动 compact。
</quote-container>

## 5. 性能

- **DELETE：**开启 secure_delete 后，单次删除耗时增加 15%~35%（取决于数据块大小和 erase_mode）。建议仅对敏感表开启。
- **查询 / 写入：**完全无影响。

## 6. 安全

1. 用户密码存储在 mnode 中，密码信息在密码被删除后，直接更新磁盘文件，不需要进行数据覆盖。
2. 安全删除模式下， delete 命令返回结果后，敏感数据所在的物理存储位置已经被无用信息覆盖，不可恢复。
3. 对敏感数据定期进行检查并清理，确保旧数据彻底删除干净。
4. 对于强制覆盖删除与定期清理都需要记录审计日志。

## 7. 兼容性

不涉及，不影响现有业务SQL的正常执行，与现有版本、功能完全兼容。

## 8. 运维

无。

## 9. 使用场景

对于敏感数据开启，防止敏感数据删除后被恢复，甚至导致泄漏。

## 10. 约束和限制

无

## 11. 常见错误和排查

无

## 12. 可观测性

1. SHOW CREATE TABLE/SHOW DATABASES 会显示 secure_delete 属性。
2. 对于安全删除和定期清理，均记录审计日志。

## 13. 安装和卸载

无。

## 14. 文档

修改官网文档

## 15. 参考文档

[支持敏感数据删除后的强制覆盖 - RS](https://taosdata.feishu.cn/wiki/IG5dwidlPisp5KkCXLucpKIxn9e)
https://docs.taosdata.com/reference/taos-sql/database/

## 16. 附录

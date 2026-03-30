# OPC CSV 时间配置 Test (Obsolete)

## 1. Jira

TS-3997


## 2. Limitation

- 

## 3. Functional Case

| Type | Description | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- |
| sanity | 使用没有配置 ts 和 rts 的配置文件。 | 运行失败，提示需配置时间 | Pass |  |
|  | 使用只包含 ts 的配置文件。 | 数据成功入库，表结构 ts 为主键列。 | Pass |  |
|  | 使用只包含 rts 的配置文件。 | 数据成功入库，表结构 rts 为主键列。 | Pass |  |
|  | 使用顺序为 ts, rts 的配置文件。 | 数据成功入库，表结构 包含 ts 和 rts 列，ts 为主键列。 | Pass |  |
|  | 使用顺序为 rts, ts 的配置文件。 | 数据成功入库，表结构包含 rts 和 ts 列，rts 为主键列。 | Pass |  |
|  | 使用顺序为 rts, ts 的配置文件，并对列进行重命名。 | 数据成功入库，表结构包含 rts 和 ts 的重命名列列，rts 为主键列。 | Pass |  |

## 4. Issue

TD-27842

## 5. Reliability

## 6. Performance

## 7. Compatibility

请注意 ts、rts 两列必须配置一列的修改导致与之前的不再兼容，如果之前版本有配置没有时间列配置的任务在更新该版本后将无法启动。

## 8. Reference

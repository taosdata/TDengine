# blob 对外API的改动

### 1. 最大SQL的限制在引擎内部支持。 

### 2. 业务侧支持BLOB类型

Blob 类型是 TSDB_DATA_TYPE_BLOB，不支持 tag。

### 3. stmt/stmt2 接口和业务侧没有关系

stmt 不支持 blob 类型，stmt2 支持 blob 类型。

### 4. raw block 的格式改动（黄色高亮部分）

```sql
// +------------------+--------------+--------------+------------------+-----------------+-------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// |  version         | total length | total rows    |  total columns  |   flag  seg     |  group id         | col1_schema(type+bytes) | col2_schema(type+bytes) | col3_schema(type+bytes)... | column#1 length, column#2 length...| col1 bitmap or col1 offset | col1 data | col2 bitmap or col2 offset  | col2 data | ....
// |  sizeof(int32_t) |sizeof(int32) | sizeof(int32) |  sizeof(int32)  |  sizeof(int32)  |  sizeof(uint64_t) |           (sizeof(int8_t)+sizeof(int32_t))*numOfCols                           | sizeof(int32_t) * numOfCols        | 
// +------------------+--------------+--------------+------------------+-----------------+-------------------+------+------------------------------------+----------
```

#### 4.1 Raw block的数据结构如上图所示，具体描述如下：

- 第一个字段：版本号，固定大小，可忽略，占用4个字节
- 第二个字段：raw block数据的总长度，占用4个字节
- 第三个字段：总行数，占用4个字节
- 第四个字段：总列数，占用4个字节
- 第五个字段：flag，固定大小，可忽略，占用4个字节
- 第六个字段：group id，block分组的id，可忽略，占用8个字节 
- 第七个字段：所有列的schema，每个列包含类型（1个字节）+所需大小（4个字节）
  如果类型为decimal(17 或者 21)，那么这里将`所需大小`的4字节拆开, 其中第一个1字节存储所需大小(8/16), 后两个字节分别存储`precision和scale`. （3.3.6.0版本开始）
  |___bytes___|__empty__|___prec___|__scale___|.
- 第八个字段：每列数据长度
- 第九个字段：
  - 每列数据内容，具体分变长的string类型和固定长度的类型。
  - **变长的类型，通过前面每行的offset来标记位置，offset=-1，表示该行为NULL，****变长数据（非BLOB类型）前两字节为长度，BLOB类型是前四个节点为长度， 后面为真实数据**。
  - 固定长度的类型，通过bitmap来标记，bit位为1表示该行为NULL，根据固定长度获取真实数据（比如int32类型占4个字节固定长度）
```cpp
DLL_EXPORT int         taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData);
DLL_EXPORT void        taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);
DLL_EXPORT const void *taos_get_raw_block(TAOS_RES *res);
DLL_EXPORT int32_t     tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw);
DLL_EXPORT int32_t     tmq_write_raw(TAOS *taos, tmq_raw_data raw);
DLL_EXPORT int         taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char *tbname);
DLL_EXPORT int         taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid);
DLL_EXPORT int         taos_write_raw_block_with_fields(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields, int numFields);
DLL_EXPORT int         taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname, TAOS_FIELD *fields, int numFields, int64_t reqid);
```

### 5. 其他API（和业务侧无关）

```c
int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) 
int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields)  
```

# TAOS SQL

本文档说明 TAOS SQL 支持的语法规则、主要查询功能、支持的 SQL 查询函数，以及常用技巧等内容。阅读本文档需要读者具有基本的 SQL 语言的基础。

TAOS SQL 是用户对 TDengine 进行数据写入和查询的主要工具。TAOS SQL 为了便于用户快速上手，在一定程度上提供类似于标准 SQL 类似的风格和模式。严格意义上，TAOS SQL 并不是也不试图提供 SQL 标准的语法。此外，由于 TDengine 针对的时序性结构化数据不提供删除功能，因此在 TAO SQL 中不提供数据删除的相关功能。

TAOS SQL 不支持关键字的缩写，例如 DESCRIBE 不能缩写为 DESC。 

本章节 SQL 语法遵循如下约定：

- < > 里的内容是用户需要输入的，但不要输入 <> 本身
- [ ] 表示内容为可选项，但不能输入 [] 本身
- | 表示多选一，选择其中一个即可，但不能输入 | 本身
- … 表示前面的项可重复多个

为更好地说明 SQL 语法的规则及其特点，本文假设存在一个数据集。以智能电表(meters)为例，假设每个智能电表采集电流、电压、相位三个量。其建模如下：
```mysql
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```
数据集包含 4 个智能电表的数据，按照 TDengine 的建模规则，对应 4 个子表，其名称分别是 d1001, d1002, d1003, d1004。

## <a class="anchor" id="data-type"></a>支持的数据类型

使用 TDengine，最重要的是时间戳。创建并插入记录、查询历史记录的时候，均需要指定时间戳。时间戳有如下规则：

- 时间格式为 ```YYYY-MM-DD HH:mm:ss.MS```，默认时间分辨率为毫秒。比如：```2017-08-12 18:25:58.128```
- 内部函数 now 是客户端的当前时间
- 插入记录时，如果时间戳为 now，插入数据时使用提交这条记录的客户端的当前时间
- Epoch Time：时间戳也可以是一个长整数，表示从格林威治时间 1970-01-01 00:00:00.000 (UTC/GMT) 开始的毫秒数（相应地，如果所在 Database 的时间精度设置为“微秒”，则长整型格式的时间戳含义也就对应于从格林威治时间 1970-01-01 00:00:00.000 (UTC/GMT) 开始的微秒数；纳秒精度的逻辑也是类似的。）
- 时间可以加减，比如 now-2h，表明查询时刻向前推 2 个小时（最近 2 小时）。数字后面的时间单位可以是 b(纳秒)、u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。 比如 `select * from t1 where ts > now-2w and ts <= now-1w`，表示查询两周前整整一周的数据。在指定降频操作（down sampling）的时间窗口（interval）时，时间单位还可以使用 n(自然月) 和 y(自然年)。

TDengine 缺省的时间戳是毫秒精度，但通过在 CREATE DATABASE 时传递的 PRECISION 参数就可以支持微秒和纳秒。（从 2.1.5.0 版本开始支持纳秒精度）

在TDengine中，普通表的数据模型中可使用以下 10 种数据类型。 

| #    | **类型**          | **Bytes** | **说明**                                                     |
| ---- | :-------: | ------ | ------------------------------------------------------------ |
| 1    | TIMESTAMP | 8      | 时间戳。缺省精度毫秒，可支持微秒和纳秒。从格林威治时间 1970-01-01 00:00:00.000 (UTC/GMT) 开始，计时不能早于该时间。（从 2.0.18.0 版本开始，已经去除了这一时间范围限制）（从 2.1.5.0 版本开始支持纳秒精度） |
| 2    |    INT    | 4      | 整型，范围 [-2^31+1,   2^31-1], -2^31 用作 NULL           |
| 3    |  BIGINT   | 8      | 长整型，范围 [-2^63+1,   2^63-1], -2^63 用于 NULL                                 |
| 4    |   FLOAT   | 4      | 浮点型，有效位数 6-7，范围 [-3.4E38, 3.4E38]                  |
| 5    |  DOUBLE   | 8      | 双精度浮点型，有效位数 15-16，范围 [-1.7E308, 1.7E308]      |
| 6    |  BINARY   | 自定义 | 记录单字节字符串，建议只用于处理 ASCII 可见字符，中文等多字节字符需使用 nchar。理论上，最长可以有 16374 字节，但由于每行数据最多 16K 字节，实际上限一般小于理论值。binary 仅支持字符串输入，字符串两端需使用单引号引用。使用时须指定大小，如 binary(20) 定义了最长为 20 个单字节字符的字符串，每个字符占 1 byte 的存储空间，总共固定占用 20 bytes 的空间，此时如果用户字符串超出 20 字节将会报错。对于字符串内的单引号，可以用转义字符反斜线加单引号来表示，即 `\’`。 |
| 7    | SMALLINT  | 2      | 短整型， 范围 [-32767, 32767], -32768 用于 NULL                                |
| 8    |  TINYINT  | 1      | 单字节整型，范围 [-127, 127], -128 用于 NULL                                |
| 9    |   BOOL    | 1      | 布尔型，{true, false}                                      |
| 10   |   NCHAR   | 自定义 | 记录包含多字节字符在内的字符串，如中文字符。每个 nchar 字符占用 4 bytes 的存储空间。字符串两端使用单引号引用，字符串内的单引号需用转义字符 `\’`。nchar 使用时须指定字符串大小，类型为 nchar(10) 的列表示此列的字符串最多存储 10 个 nchar 字符，会固定占用 40 bytes 的空间。如果用户字符串长度超出声明长度，将会报错。 |
<!-- REPLACE_OPEN_TO_ENTERPRISE__COLUMN_TYPE_ADDONS -->

**Tips**:
1. TDengine 对 SQL 语句中的英文字符不区分大小写，自动转化为小写执行。因此用户大小写敏感的字符串及密码，需要使用单引号将字符串引起来。
2. **注意**，虽然 Binary 类型在底层存储上支持字节型的二进制字符，但不同编程语言对二进制数据的处理方式并不保证一致，因此建议在 Binary 类型中只存储 ASCII 可见字符，而避免存储不可见字符。多字节的数据，例如中文字符，则需要使用 nchar 类型进行保存。如果强行使用 Binary 类型保存中文字符，虽然有时也能正常读写，但并不带有字符集信息，很容易出现数据乱码甚至数据损坏等情况。

## <a class="anchor" id="management"></a>数据库管理

- **创建数据库**  

    ```mysql
    CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [DAYS days] [UPDATE 1];
    ```
    说明：<!-- 注意：上一行中的 SQL 语句在企业版文档中会被替换，因此修改此语句的话，需要修改企业版文档的替换字典键值！！ -->
 
    1) KEEP是该数据库的数据保留多长天数，缺省是3650天(10年)，数据库会自动删除超过时限的数据；<!-- REPLACE_OPEN_TO_ENTERPRISE__KEEP_PARAM_DESCRIPTION -->
 
    2) UPDATE 标志数据库支持更新相同时间戳数据；
 
    3) 数据库名最大长度为33；
 
    4) 一条SQL 语句的最大长度为65480个字符；
 
    5) 数据库还有更多与存储相关的配置参数，请参见 [服务端配置](https://www.taosdata.com/cn/documentation/administrator#config) 章节。

- **显示系统当前参数**

    ```mysql
    SHOW VARIABLES;
    ```

- **使用数据库**

    ```mysql
    USE db_name;
    ```
    使用/切换数据库（在 RESTful 连接方式下无效）。

- **删除数据库**
    ```mysql
    DROP DATABASE [IF EXISTS] db_name;
    ```
    删除数据库。指定 Database 所包含的全部数据表将被删除，谨慎使用！

- **修改数据库参数**
    ```mysql
    ALTER DATABASE db_name COMP 2;
    ```
    COMP 参数是指修改数据库文件压缩标志位，缺省值为 2，取值范围为 [0, 2]。0 表示不压缩，1 表示一阶段压缩，2 表示两阶段压缩。

    ```mysql
    ALTER DATABASE db_name REPLICA 2;
    ```
    REPLICA 参数是指修改数据库副本数，取值范围 [1, 3]。在集群中使用，副本数必须小于或等于 DNODE 的数目。

    ```mysql
    ALTER DATABASE db_name KEEP 365;
    ```
    KEEP 参数是指修改数据文件保存的天数，缺省值为 3650，取值范围 [days, 365000]，必须大于或等于 days 参数值。

    ```mysql
    ALTER DATABASE db_name QUORUM 2;
    ```
    QUORUM 参数是指数据写入成功所需要的确认数，取值范围 [1, 2]。对于异步复制，quorum 设为 1，具有 master 角色的虚拟节点自己确认即可。对于同步复制，需要至少大于等于 2。原则上，Quorum >= 1 并且 Quorum <= replica(副本数)，这个参数在启动一个同步模块实例时需要提供。

    ```mysql
    ALTER DATABASE db_name BLOCKS 100;
    ```
    BLOCKS 参数是每个 VNODE (TSDB) 中有多少 cache 大小的内存块，因此一个 VNODE 的用的内存大小粗略为（cache * blocks）。取值范围 [3, 1000]。

    ```mysql
    ALTER DATABASE db_name CACHELAST 0;
    ```
    CACHELAST 参数控制是否在内存中缓存子表的最近数据。缺省值为 0，取值范围 [0, 1, 2, 3]。其中 0 表示不缓存，1 表示缓存子表最近一行数据，2 表示缓存子表每一列的最近的非 NULL 值，3 表示同时打开缓存最近行和列功能。（从 2.0.11.0 版本开始支持参数值 [0, 1]，从 2.1.2.0 版本开始支持参数值 [0, 1, 2, 3]。）  
    说明：缓存最近行，将显著改善 LAST_ROW 函数的性能表现；缓存每列的最近非 NULL 值，将显著改善无特殊影响（WHERE、ORDER BY、GROUP BY、INTERVAL）下的 LAST 函数的性能表现。

    **Tips**: 以上所有参数修改后都可以用show databases来确认是否修改成功。另外，从 2.1.3.0 版本开始，修改这些参数后无需重启服务器即可生效。

- **显示系统所有数据库**

    ```mysql
    SHOW DATABASES;
    ```

- **显示一个数据库的创建语句**

    ```mysql
    SHOW CREATE DATABASE db_name;
    ```
    常用于数据库迁移。对一个已经存在的数据库，返回其创建语句；在另一个集群中执行该语句，就能得到一个设置完全相同的 Database。


## <a class="anchor" id="table"></a>表管理

- **创建数据表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]);
    ```
    说明：

    1) 表的第一个字段必须是 TIMESTAMP，并且系统自动将其设为主键；

    2) 表名最大长度为 192；

    3) 表的每行长度不能超过 16k 个字符;（注意：每个 BINARY/NCHAR 类型的列还会额外占用 2 个字节的存储位置）

    4) 子表名只能由字母、数字和下划线组成，且不能以数字开头

    5) 使用数据类型 binary 或 nchar，需指定其最长的字节数，如 binary(20)，表示 20 字节；

- **以超级表为模板创建数据表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
    ```
    以指定的超级表为模板，指定 TAGS 的值来创建数据表。

- **以超级表为模板创建数据表，并指定具体的 TAGS 列**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
    ```
    以指定的超级表为模板，指定一部分 TAGS 列的值来创建数据表（没被指定的 TAGS 列会设为空值）。  
    说明：从 2.0.17.0 版本开始支持这种方式。在之前的版本中，不允许指定 TAGS 列，而必须显式给出所有 TAGS 列的取值。

- **批量创建数据表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
    ```
    以更快的速度批量创建大量数据表（服务器端 2.0.14 及以上版本）。
    
    说明：

    1）批量建表方式要求数据表必须以超级表为模板。

    2）在不超出 SQL 语句长度限制的前提下，单条语句中的建表数量建议控制在 1000～3000 之间，将会获得比较理想的建表速度。

- **删除数据表**

    ```mysql
    DROP TABLE [IF EXISTS] tb_name;
    ```

- **显示当前数据库下的所有数据表信息**

    ```mysql
    SHOW TABLES [LIKE tb_name_wildcar];
    ```

    显示当前数据库下的所有数据表信息。

    说明：可在 like 中使用通配符进行名称的匹配，这一通配符字符串最长不能超过 20 字节。（ 从 2.1.6.1 版本开始，通配符字符串的长度放宽到了 100 字节，并可以通过 taos.cfg 中的 maxWildCardsLength 参数来配置这一长度限制。但不建议使用太长的通配符字符串，将有可能严重影响 LIKE 操作的执行性能。）

    通配符匹配：1）'%'（百分号）匹配0到任意个字符；2）'\_'下划线匹配单个任意字符。

- **显示一个数据表的创建语句**

    ```mysql
    SHOW CREATE TABLE tb_name;
    ```
    常用于数据库迁移。对一个已经存在的数据表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的数据表。

- **在线修改显示字符宽度**

    ```mysql
    SET MAX_BINARY_DISPLAY_WIDTH <nn>;
    ```
    如显示的内容后面以...结尾时，表示该内容已被截断，可通过本命令修改显示字符宽度以显示完整的内容。

- **获取表的结构信息**

    ```mysql
    DESCRIBE tb_name;
    ```

- **表增加列**

    ```mysql
    ALTER TABLE tb_name ADD COLUMN field_name data_type;
    ```
    说明：

    1) 列的最大个数为1024，最小个数为2；

    2) 列名最大长度为64。

- **表删除列**

    ```mysql
    ALTER TABLE tb_name DROP COLUMN field_name; 
    ```
    如果表是通过超级表创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构。

- **表修改列宽**

    ```mysql
    ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length); 
    ```
    如果数据列的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）  
    如果表是通过超级表创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构。

## <a class="anchor" id="super-table"></a>超级表STable管理

注意：在 2.0.15.0 及以后的版本中，开始支持 STABLE 保留字。也即，在本节后文的指令说明中，CREATE、DROP、ALTER 三个指令在老版本中保留字需写作 TABLE 而不是 STABLE。

- **创建超级表**

    ```mysql
    CREATE STABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
    ```
    创建 STable，与创建表的 SQL 语法相似，但需要指定 TAGS 字段的名称和类型

    说明：

    1) TAGS 列的数据类型不能是 timestamp 类型；（从 2.1.3.0 版本开始，TAGS 列中支持使用 timestamp 类型，但需注意在 TAGS 中的 timestamp 列写入数据时需要提供给定值，而暂不支持四则运算，例如 `NOW + 10s` 这类表达式）

    2) TAGS 列名不能与其他列名相同；

    3) TAGS 列名不能为预留关键字（参见：[参数限制与保留关键字](https://www.taosdata.com/cn/documentation/administrator#keywords) 章节）；

    4) TAGS 最多允许 128 个，至少 1 个，总长度不超过 16 KB。

- **删除超级表**

    ```mysql
    DROP STABLE [IF EXISTS] stb_name;
    ```
    删除 STable 会自动删除通过 STable 创建的子表。

- **显示当前数据库下的所有超级表信息**

    ```mysql
    SHOW STABLES [LIKE tb_name_wildcard];
    ```
    查看数据库内全部 STable，及其相关信息，包括 STable 的名称、创建时间、列数量、标签（TAG）数量、通过该 STable 建表的数量。

- **显示一个超级表的创建语句**

    ```mysql
    SHOW CREATE STABLE stb_name;
    ```
    常用于数据库迁移。对一个已经存在的超级表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的超级表。

- **获取超级表的结构信息**

    ```mysql
    DESCRIBE stb_name;
    ```

- **超级表增加列**

    ```mysql
    ALTER STABLE stb_name ADD COLUMN field_name data_type;
    ```

- **超级表删除列**

    ```mysql
    ALTER STABLE stb_name DROP COLUMN field_name; 
    ```

- **超级表修改列宽**

    ```mysql
    ALTER STABLE stb_name MODIFY COLUMN field_name data_type(length); 
    ```
    如果数据列的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）

## <a class="anchor" id="tags"></a>超级表 STable 中 TAG 管理

- **添加标签**

    ```mysql
    ALTER STABLE stb_name ADD TAG new_tag_name tag_type;
    ```
    为 STable 增加一个新的标签，并指定新标签的类型。标签总数不能超过 128 个，总长度不超过 16k 个字符。

- **删除标签**

    ```mysql
    ALTER STABLE stb_name DROP TAG tag_name;
    ```
    删除超级表的一个标签，从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。

- **修改标签名**

    ```mysql
    ALTER STABLE stb_name CHANGE TAG old_tag_name new_tag_name;
    ```
    修改超级表的标签名，从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。

- **修改标签列宽度**

    ```mysql
    ALTER STABLE stb_name MODIFY TAG tag_name data_type(length); 
    ```
    如果标签的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）

- **修改子表标签值**

    ```mysql
    ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
    ```
    说明：除了更新标签的值的操作是针对子表进行，其他所有的标签操作（添加标签、删除标签等）均只能作用于 STable，不能对单个子表操作。对 STable 添加标签以后，依托于该 STable 建立的所有表将自动增加了一个标签，所有新增标签的默认值都是 NULL。

## <a class="anchor" id="insert"></a>数据写入

### 写入语法：

```mysql
INSERT INTO
    tb_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [tb2_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];
```

### 详细描述及示例：

- **插入一条或多条记录**  
    指定已经创建好的数据子表的表名，并通过 VALUES 关键字提供一行或多行数据，即可向数据库写入这些数据。例如，执行如下语句可以写入一行记录：
    ```mysql
    INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
    ```
    或者，可以通过如下语句写入两行记录：  
    ```mysql
    INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
    ```
    **注意：**  
    1）在第二个例子中，两行记录的首列时间戳使用了不同格式的写法。其中字符串格式的时间戳写法不受所在 DATABASE 的时间精度设置影响；而长整形格式的时间戳写法会受到所在 DATABASE 的时间精度设置影响——例子中的时间戳在毫秒精度下可以写作 1626164208000，而如果是在微秒精度设置下就需要写为 1626164208000000，纳秒精度设置下需要写为 1626164208000000000。  
    2）在使用“插入多条记录”方式写入数据时，不能把第一列的时间戳取值都设为 NOW，否则会导致语句中的多条记录使用相同的时间戳，于是就可能出现相互覆盖以致这些数据行无法全部被正确保存。其原因在于，NOW 函数在执行中会被解析为所在 SQL 语句的实际执行时间，出现在同一语句中的多个 NOW 标记也就会被替换为完全相同的时间戳取值。  
    3）允许插入的最老记录的时间戳，是相对于当前服务器时间，减去配置的 keep 值（数据保留的天数）；允许插入的最新记录的时间戳，是相对于当前服务器时间，加上配置的 days 值（数据文件存储数据的时间跨度，单位为天）。keep 和 days 都是可以在创建数据库时指定的，缺省值分别是 3650 天和 10 天。

- **插入记录，数据对应到指定的列**  
    向数据子表中插入记录时，无论插入一行还是多行，都可以让数据对应到指定的列。对于 SQL 语句中没有出现的列，数据库将自动填充为 NULL。主键（时间戳）不能为 NULL。例如：
    ```mysql
    INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
    ```
    **说明：**如果不指定列，也即使用全列模式——那么在 VALUES 部分提供的数据，必须为数据表的每个列都显式地提供数据。全列模式写入速度会远快于指定列，因此建议尽可能采用全列写入方式，此时空列可以填入 NULL。

- **向多个表插入记录**  
    可以在一条语句中，分别向多个表插入一条或多条记录，并且也可以在插入过程中指定列。例如：
    ```mysql
    INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
                d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31）;
    ```

- <a class="anchor" id="auto_create_table"></a>**插入记录时自动建表**  
    如果用户在写数据时并不确定某个表是否存在，此时可以在写入数据时使用自动建表语法来创建不存在的表，若该表已存在则不会建立新表。自动建表时，要求必须以超级表为模板，并写明数据表的 TAGS 取值。例如：  
    ```mysql
    INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
    ```
    也可以在自动建表时，只是指定部分 TAGS 列的取值，未被指定的 TAGS 列将置为 NULL。例如：  
    ```mysql
    INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
    ```
    自动建表语法也支持在一条语句中向多个表插入记录。例如：  
    ```mysql
    INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
                d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
                d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
    ```
    **说明：**在 2.0.20.5 版本之前，在使用自动建表语法并指定列时，子表的列名必须紧跟在子表名称后面，而不能如例子里那样放在 TAGS 和 VALUES 之间。从 2.0.20.5 版本开始，两种写法都可以，但不能在一条 SQL 语句中混用，否则会报语法错误。

- **插入来自文件的数据记录**  
    除了使用 VALUES 关键字插入一行或多行数据外，也可以把要写入的数据放在 CSV 文件中（英文逗号分隔、英文单引号括住每个值）供 SQL 指令读取。其中 CSV 文件无需表头。例如，如果 /tmp/csvfile.csv 文件的内容为：  
    ```
    '2021-07-13 14:07:34.630', '10.2', '219', '0.32'
    '2021-07-13 14:07:35.779', '10.15', '217', '0.33'
    ```
    那么通过如下指令可以把这个文件中的数据写入子表中：  
    ```mysql
    INSERT INTO d1001 FILE '/tmp/csvfile.csv';
    ```

- **插入来自文件的数据记录，并自动建表**  
    从 2.1.5.0 版本开始，支持在插入来自 CSV 文件的数据时，以超级表为模板来自动创建不存在的数据表。例如：  
    ```mysql
    INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) FILE '/tmp/csvfile.csv';
    ```
    也可以在一条语句中向多个表以自动建表的方式插入记录。例如：  
    ```mysql
    INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) FILE '/tmp/csvfile_21001.csv'
                d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
    ```

**历史记录写入**：可使用IMPORT或者INSERT命令，IMPORT的语法，功能与INSERT完全一样。

**说明：**针对 insert 类型的 SQL 语句，我们采用的流式解析策略，在发现后面的错误之前，前面正确的部分 SQL 仍会执行。下面的 SQL 中，INSERT 语句是无效的，但是 d1001 仍会被创建。

```mysql
taos> CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
Query OK, 0 row(s) affected (0.008245s)

taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2020-08-06 17:50:27.831 |       4 |      2 |           0 |
Query OK, 1 row(s) in set (0.001029s)

taos> SHOW TABLES;
Query OK, 0 row(s) in set (0.000946s)

taos> INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');

DB error: invalid SQL: 'a' (invalid timestamp) (0.039494s)

taos> SHOW TABLES;
           table_name           |      created_time       | columns |          stable_name           |
======================================================================================================
 d1001                          | 2020-08-06 17:52:02.097 |       4 | meters                         |
Query OK, 1 row(s) in set (0.001091s)
```

## <a class="anchor" id="select"></a>数据查询

### 查询语法：

```mysql
SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [SESSION(ts_col, tol_val)]
    [STATE_WINDOW(col)]
    [INTERVAL(interval_val [, interval_offset]) [SLIDING sliding_val]]
    [FILL(fill_mod_and_val)]
    [GROUP BY col_list]
    [ORDER BY col_list { DESC | ASC }]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file];
```

#### 通配符

通配符 * 可以用于代指全部列。对于普通表，结果中只有普通列。
```mysql
taos> SELECT * FROM d1001;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
Query OK, 3 row(s) in set (0.001165s)
```

在针对超级表，通配符包含 _标签列_ 。
```mysql
taos> SELECT * FROM meters;
           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
=====================================================================================================================================
 2018-10-03 14:38:05.500 |             11.80000 |         221 |              0.28000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:16.600 |             13.40000 |         223 |              0.29000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:05.000 |             10.80000 |         223 |              0.29000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:06.500 |             11.50000 |         221 |              0.35000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:04.000 |             10.20000 |         220 |              0.23000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:16.650 |             10.30000 |         218 |              0.25000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 | Beijing.Chaoyang               |           2 |
Query OK, 9 row(s) in set (0.002022s)
```

通配符支持表名前缀，以下两个SQL语句均为返回全部的列：
```mysql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```
在JOIN查询中，带前缀的\*和不带前缀\*返回的结果有差别， \*返回全部表的所有列数据（不包含标签），带前缀的通配符，则只返回该表的列数据。
```mysql
taos> SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
           ts            | current |   voltage   |    phase     |           ts            | current |   voltage   |    phase     |
==================================================================================================================================
 2018-10-03 14:38:05.000 | 10.30000|         219 |      0.31000 | 2018-10-03 14:38:05.000 | 10.80000|         223 |      0.29000 |
Query OK, 1 row(s) in set (0.017385s)
```
```mysql
taos> SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
Query OK, 1 row(s) in set (0.020443s)
```

在使用SQL函数来进行查询的过程中，部分SQL函数支持通配符操作。其中的区别在于：
```count(*)```函数只返回一列。```first```、```last```、```last_row```函数则是返回全部列。

```mysql
taos> SELECT COUNT(*) FROM d1001;
       count(*)        |
========================
                     3 |
Query OK, 1 row(s) in set (0.001035s)
```

```mysql
taos> SELECT FIRST(*) FROM d1001;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |            219 |              0.31000 |
Query OK, 1 row(s) in set (0.000849s)
```

#### 标签列

从 2.0.14 版本开始，支持在普通表的查询中指定 _标签列_，且标签列的值会与普通列的数据一起返回。
```mysql
taos> SELECT location, groupid, current FROM d1001 LIMIT 2;
            location            |   groupid   |       current        |
======================================================================
 Beijing.Chaoyang               |           2 |             10.30000 |
 Beijing.Chaoyang               |           2 |             12.60000 |
Query OK, 2 row(s) in set (0.003112s)
```

注意：普通表的通配符 * 中并不包含 _标签列_。

##### 获取标签列的去重取值

从 2.0.15 版本开始，支持在超级表查询标签列时，指定 DISTINCT 关键字，这样将返回指定标签列的所有不重复取值。
```mysql
SELECT DISTINCT tag_name FROM stb_name;
```

注意：目前 DISTINCT 关键字只支持对超级表的标签列进行去重，而不能用于普通列。



#### 结果集列名

```SELECT```子句中，如果不指定返回结果集合的列名，结果集列名称默认使用```SELECT```子句中的表达式名称作为列名称。此外，用户可使用```AS```来重命名返回结果集合中列的名称。例如：
```mysql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
           ts            |     primary_key_ts      |
====================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:05.000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:15.000 |
 2018-10-03 14:38:16.800 | 2018-10-03 14:38:16.800 |
Query OK, 3 row(s) in set (0.001191s)
```
但是针对```first(*)```、```last(*)```、```last_row(*)```不支持针对单列的重命名。

#### 隐式结果列

```Select_exprs```可以是表所属列的列名，也可以是基于列的函数表达式或计算式，数量的上限256个。当用户使用了```interval```或```group by tags```的子句以后，在最后返回结果中会强制返回时间戳列（第一列）和group by子句中的标签列。后续的版本中可以支持关闭group by子句中隐式列的输出，列输出完全由select子句控制。

#### 表（超级表）列表

FROM关键字后面可以是若干个表（超级表）列表，也可以是子查询的结果。
如果没有指定用户的当前数据库，可以在表名称之前使用数据库的名称来指定表所属的数据库。例如：```power.d1001``` 方式来跨库使用表。
```mysql
SELECT * FROM power.d1001;
------------------------------
USE power;
SELECT * FROM d1001;
```

#### 特殊功能

部分特殊的查询功能可以不使用FROM子句执行。获取当前所在的数据库 database()： 
```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 power                          |
Query OK, 1 row(s) in set (0.000079s)
```
如果登录的时候没有指定默认数据库，且没有使用```USE```命令切换数据，则返回NULL。
```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 NULL                           |
Query OK, 1 row(s) in set (0.000184s)
```
获取服务器和客户端版本号：
```mysql
taos> SELECT CLIENT_VERSION();
 client_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000070s)

taos> SELECT SERVER_VERSION();
 server_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000077s)
```
服务器状态检测语句。如果服务器正常，返回一个数字（例如 1）。如果服务器异常，返回error code。该SQL语法能兼容连接池对于TDengine状态的检查及第三方工具对于数据库服务器状态的检查。并可以避免出现使用了错误的心跳检测SQL语句导致的连接池连接丢失的问题。
```mysql
taos> SELECT SERVER_STATUS();
 server_status() |
==================
               1 |
Query OK, 1 row(s) in set (0.000074s)

taos> SELECT SERVER_STATUS() AS status;
   status    |
==============
           1 |
Query OK, 1 row(s) in set (0.000081s)
```

#### TAOS SQL中特殊关键词

 >   TBNAME： 在超级表查询中可视为一个特殊的标签，代表查询涉及的子表名<br>
    \_c0: 表示表（超级表）的第一列

#### 小技巧

获取一个超级表所有的子表名及相关的标签信息：
```mysql
SELECT TBNAME, location FROM meters;
```
统计超级表下辖子表数量：
```mysql
SELECT COUNT(TBNAME) FROM meters;
```
以上两个查询均只支持在WHERE条件子句中添加针对标签（TAGS）的过滤条件。例如：
```mysql
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | Beijing.Haidian                |
 d1003                          | Beijing.Haidian                |
 d1002                          | Beijing.Chaoyang               |
 d1001                          | Beijing.Chaoyang               |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
```

- 可以使用 * 返回所有列，或指定列名。可以对数字列进行四则运算，可以给输出的列取列名。
  * 暂不支持含列名的四则运算表达式用于条件过滤算子（例如，不支持 `where a*2>6;`，但可以写 `where a>6/2;`）。
  * 暂不支持含列名的四则运算表达式作为 SQL 函数的应用对象（例如，不支持 `select min(2*a) from t;`，但可以写 `select 2*min(a) from t;`）。
- WHERE 语句可以使用各种逻辑判断来过滤数字值，或使用通配符来过滤字符串。
- 输出结果缺省按首列时间戳升序排序，但可以指定按降序排序( _c0 指首列时间戳)。使用 ORDER BY 对其他字段进行排序为非法操作。
- 参数 LIMIT 控制输出条数，OFFSET 指定从第几条开始输出。LIMIT/OFFSET 对结果集的执行顺序在 ORDER BY 之后。且 `LIMIT 5 OFFSET 2` 可以简写为 `LIMIT 2, 5`。
  * 在有 GROUP BY 子句的情况下，LIMIT 参数控制的是每个分组中至多允许输出的条数。
- 参数 SLIMIT 控制由 GROUP BY 指令划分的分组中，至多允许输出几个分组的数据。且 `SLIMIT 5 SOFFSET 2` 可以简写为 `SLIMIT 2, 5`。
- 通过 “>>” 输出结果可以导出到指定文件。

### 支持的条件过滤操作

| **Operation**   | **Note**                      | **Applicable Data Types**                 |
| --------------- | ----------------------------- | ----------------------------------------- |
| >               | larger than                   | **`timestamp`** and all numeric types     |
| <               | smaller than                  | **`timestamp`** and all numeric types     |
| >=              | larger than or equal to       | **`timestamp`** and all numeric types     |
| <=              | smaller than or equal to      | **`timestamp`** and all numeric types     |
| =               | equal to                      | all types                                 |
| <>              | not equal to                  | all types                                 |
| between and     | within a certain range        | **`timestamp`** and all numeric types     |
| in              | matches any value in a set    | all types except first column `timestamp` |
| %               | match with any char sequences | **`binary`** **`nchar`**                  |
| _               | match with a single char      | **`binary`** **`nchar`**                  |

1. <> 算子也可以写为 != ，请注意，这个算子不能用于数据表第一列的 timestamp 字段。
2. 同时进行多个字段的范围过滤，需要使用关键词 AND 来连接不同的查询条件，暂不支持 OR 连接的不同列之间的查询过滤条件。
3. 针对单一字段的过滤，如果是时间过滤条件，则一条语句中只支持设定一个；但针对其他的（普通）列或标签列，则可以使用 `OR` 关键字进行组合条件的查询过滤。例如： `((value > 20 AND value < 30) OR (value < 12))`。
4. 从 2.0.17.0 版本开始，条件过滤开始支持 BETWEEN AND 语法，例如 `WHERE col2 BETWEEN 1.5 AND 3.25` 表示查询条件为“1.5 ≤ col2 ≤ 3.25”。
5. 从 2.1.4.0 版本开始，条件过滤开始支持 IN 算子，例如 `WHERE city IN ('Beijing', 'Shanghai')`。说明：BOOL 类型写作 `{true, false}` 或 `{0, 1}` 均可，但不能写作 0、1 之外的整数；FLOAT 和 DOUBLE 类型会受到浮点数精度影响，集合内的值在精度范围内认为和数据行的值完全相等才能匹配成功；TIMESTAMP 类型支持非主键的列。<!-- REPLACE_OPEN_TO_ENTERPRISE__IN_OPERATOR_AND_UNSIGNED_INTEGER -->

<!-- 
<a class="anchor" id="having"></a>
### GROUP BY 之后的 HAVING 过滤

从 2.0.20.0 版本开始，GROUP BY 之后允许再跟一个 HAVING 子句，对成组后的各组数据再做筛选。HAVING 子句可以使用聚合函数和选择函数作为过滤条件（但暂时不支持 LEASTSQUARES、TOP、BOTTOM、LAST_ROW）。

例如，如下语句只会输出 `AVG(f1) > 0` 的分组：
```mysql
SELECT AVG(f1), SPREAD(f1, f2, st2.f1) FROM st2 WHERE f1 > 0 GROUP BY f1 HAVING AVG(f1) > 0;
```
-->

<a class="anchor" id="union"></a>
### UNION ALL 操作符

```mysql
SELECT ...
UNION ALL SELECT ...
[UNION ALL SELECT ...]
```

TDengine 支持 UNION ALL 操作符。也就是说，如果多个 SELECT 子句返回结果集的结构完全相同（列名、列类型、列数、顺序），那么可以通过 UNION ALL 把这些结果集合并到一起。目前只支持 UNION ALL 模式，也即在结果集的合并过程中是不去重的。

### SQL 示例 

- 对于下面的例子，表tb1用以下语句创建：

    ```mysql
    CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
    ```

- 查询tb1刚过去的一个小时的所有记录：

    ```mysql
    SELECT * FROM tb1 WHERE ts >= NOW - 1h;
    ```

- 查询表tb1从2018-06-01 08:00:00.000 到2018-06-02 08:00:00.000时间范围，并且col3的字符串是'nny'结尾的记录，结果按照时间戳降序：

    ```mysql
    SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
    ```

- 查询col1与col2的和，并取名complex, 时间大于2018-06-01 08:00:00.000, col2大于1.2，结果输出仅仅10条记录，从第5条开始：

    ```mysql
    SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
    ```

- 查询过去10分钟的记录，col2的值大于3.14，并且将结果输出到文件 `/home/testoutpu.csv`：

    ```mysql
    SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv;
    ```

<a class="anchor" id="functions"></a>
## SQL 函数

### 聚合函数

TDengine支持针对数据的聚合查询。提供支持的聚合和选择函数如下：

- **COUNT**
    ```mysql
    SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中记录行数或某列的非空值个数。

    返回结果数据类型：长整型INT64。

    应用字段：应用全部字段。

    适用于：**表、超级表**。

    说明：

    1）可以使用星号(\*)来替代具体的字段，使用星号(\*)返回全部记录数量。

    2）针对同一表的（不包含NULL值）字段查询结果均相同。

    3）如果统计对象是具体的列，则返回该列中非NULL值的记录数量。

    示例：
    ```mysql
    taos> SELECT COUNT(*), COUNT(voltage) FROM meters;
        count(*)        |    count(voltage)     |
    ================================================
                        9 |                     9 |
    Query OK, 1 row(s) in set (0.004475s)

    taos> SELECT COUNT(*), COUNT(voltage) FROM d1001;
        count(*)        |    count(voltage)     |
    ================================================
                        3 |                     3 |
    Query OK, 1 row(s) in set (0.001075s)
    ```

- **AVG**
    ```mysql
    SELECT AVG(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的平均值。

    返回结果数据类型：双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool字段。

    适用于：**表、超级表**。

    示例：
    ```mysql
    taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM meters;
        avg(current)        |       avg(voltage)        |        avg(phase)         |
    ====================================================================================
                11.466666751 |             220.444444444 |               0.293333333 |
    Query OK, 1 row(s) in set (0.004135s)

    taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM d1001;
        avg(current)        |       avg(voltage)        |        avg(phase)         |
    ====================================================================================
                11.733333588 |             219.333333333 |               0.316666673 |
    Query OK, 1 row(s) in set (0.000943s)
    ```

- **TWA**
    ```mysql
    SELECT TWA(field_name) FROM tb_name WHERE clause;
    ```
    功能说明：时间加权平均函数。统计表中某列在一段时间内的时间加权平均。

    返回结果数据类型：双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、（超级表）**。

    说明：从 2.1.3.0 版本开始，TWA 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

- **IRATE**
    ```mysql
    SELECT IRATE(field_name) FROM tb_name WHERE clause;
    ```
    功能说明：计算瞬时增长率。使用时间区间中最后两个样本数据来计算瞬时增长速率；如果这两个值呈递减关系，那么只取最后一个数用于计算，而不是使用二者差值。

    返回结果数据类型：双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、（超级表）**。

    说明：（从 2.1.3.0 版本开始新增此函数）IRATE 可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

- **SUM**
    ```mysql
    SELECT SUM(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的和。

    返回结果数据类型：双精度浮点数Double和长整型INT64。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    示例：
    ```mysql
    taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM meters;
        sum(current)        |     sum(voltage)      |        sum(phase)         |
    ================================================================================
                103.200000763 |                  1984 |               2.640000001 |
    Query OK, 1 row(s) in set (0.001702s)

    taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM d1001;
        sum(current)        |     sum(voltage)      |        sum(phase)         |
    ================================================================================
                35.200000763 |                   658 |               0.950000018 |
    Query OK, 1 row(s) in set (0.000980s)
    ```

- **STDDEV**
    ```mysql
    SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的均方差。

    返回结果数据类型：双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表**。（从 2.0.15.1 版本开始，本函数也支持**超级表**）

    示例：
    ```mysql
    taos> SELECT STDDEV(current) FROM d1001;
        stddev(current)      |
    ============================
                1.020892909 |
    Query OK, 1 row(s) in set (0.000915s)
    ```

- **LEASTSQUARES**
    ```mysql
    SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的值是主键（时间戳）的拟合直线方程。start_val是自变量初始值，step_val是自变量的步长值。

    返回结果数据类型：字符串表达式（斜率, 截距）。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    说明：自变量是时间戳，因变量是该列的值。

    适用于：**表**。

    示例：
    ```mysql
    taos> SELECT LEASTSQUARES(current, 1, 1) FROM d1001;
                leastsquares(current, 1, 1)             |
    =====================================================
    {slop:1.000000, intercept:9.733334}                 |
    Query OK, 1 row(s) in set (0.000921s)
    ```

### 选择函数

在使用所有的选择函数的时候，可以同时指定输出 ts 列或标签列（包括 tbname），这样就可以方便地知道被选出的值是源于哪个数据行的。

- **MIN**
    ```mysql
    SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最小值。

    返回结果数据类型：同应用的字段。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    示例：
    ```mysql
    taos> SELECT MIN(current), MIN(voltage) FROM meters;
        min(current)     | min(voltage) |
    ======================================
                10.20000 |          218 |
    Query OK, 1 row(s) in set (0.001765s)

    taos> SELECT MIN(current), MIN(voltage) FROM d1001;
        min(current)     | min(voltage) |
    ======================================
                10.30000 |          218 |
    Query OK, 1 row(s) in set (0.000950s)
    ```

- **MAX**
    ```mysql
    SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最大值。

    返回结果数据类型：同应用的字段。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    示例：
    ```mysql
    taos> SELECT MAX(current), MAX(voltage) FROM meters;
        max(current)     | max(voltage) |
    ======================================
                13.40000 |          223 |
    Query OK, 1 row(s) in set (0.001123s)

    taos> SELECT MAX(current), MAX(voltage) FROM d1001;
        max(current)     | max(voltage) |
    ======================================
                12.60000 |          221 |
    Query OK, 1 row(s) in set (0.000987s)
    ```

- **FIRST**
    ```mysql
    SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最先写入的非NULL值。

    返回结果数据类型：同应用的字段。

    应用字段：所有字段。

    适用于：**表、超级表**。

    说明：

    1）如果要返回各个列的首个（时间戳最小）非NULL值，可以使用FIRST(\*)；

    2) 如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；

    3) 如果结果集中所有列全部为NULL值，则不返回结果。

    示例：
    ```mysql
    taos> SELECT FIRST(*) FROM meters;
            first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
    =========================================================================================
    2018-10-03 14:38:04.000 |             10.20000 |            220 |              0.23000 |
    Query OK, 1 row(s) in set (0.004767s)

    taos> SELECT FIRST(current) FROM d1002;
        first(current)    |
    =======================
                10.20000 |
    Query OK, 1 row(s) in set (0.001023s)
    ```

- **LAST**
    ```mysql
    SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最后写入的非NULL值。

    返回结果数据类型：同应用的字段。

    应用字段：所有字段。

    适用于：**表、超级表**。

    说明：

    1）如果要返回各个列的最后（时间戳最大）一个非NULL值，可以使用LAST(\*)；

    2）如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；如果结果集中所有列全部为NULL值，则不返回结果。

    示例：
    ```mysql
    taos> SELECT LAST(*) FROM meters;
            last(ts)         |    last(current)     | last(voltage) |     last(phase)      |
    ========================================================================================
    2018-10-03 14:38:16.800 |             12.30000 |           221 |              0.31000 |
    Query OK, 1 row(s) in set (0.001452s)

    taos> SELECT LAST(current) FROM d1002;
        last(current)     |
    =======================
                10.30000 |
    Query OK, 1 row(s) in set (0.000843s)
    ```

- **TOP**
    ```mysql
    SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明： 统计表/超级表中某列的值最大 *k* 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。

    返回结果数据类型：同应用的字段。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    说明：

    1）*k*值取值范围1≤*k*≤100；

    2）系统同时返回该记录关联的时间戳列；

    3）限制：TOP函数不支持FILL子句。

    示例：
    ```mysql
    taos> SELECT TOP(current, 3) FROM meters;
            ts            |   top(current, 3)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.600 |             13.40000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 3 row(s) in set (0.001548s)

    taos> SELECT TOP(current, 2) FROM d1001;
            ts            |   top(current, 2)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000810s)
    ```

- **BOTTOM**
    ```mysql
    SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最小 *k* 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。

    返回结果数据类型：同应用的字段。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    说明：

    1）*k*值取值范围1≤*k*≤100；

    2）系统同时返回该记录关联的时间戳列；

    3）限制：BOTTOM函数不支持FILL子句。

    示例：
    ```mysql
    taos> SELECT BOTTOM(voltage, 2) FROM meters;
            ts            | bottom(voltage, 2) |
    ===============================================
    2018-10-03 14:38:15.000 |                218 |
    2018-10-03 14:38:16.650 |                218 |
    Query OK, 2 row(s) in set (0.001332s)

    taos> SELECT BOTTOM(current, 2) FROM d1001;
            ts            |  bottom(current, 2)  |
    =================================================
    2018-10-03 14:38:05.000 |             10.30000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000793s)
    ```

- **PERCENTILE**
    ```mysql
    SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
    ```
    功能说明：统计表中某列的值百分比分位数。

    返回结果数据类型： 双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表**。

    说明：*P*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。

    示例：
    ```mysql
    taos> SELECT PERCENTILE(current, 20) FROM d1001;
    percentile(current, 20)  |
    ============================
                11.100000191 |
    Query OK, 1 row(s) in set (0.000787s)
    ```

- **APERCENTILE**
    ```mysql
    SELECT APERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值百分比分位数，与PERCENTILE函数相似，但是返回近似结果。

    返回结果数据类型： 双精度浮点数Double。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    说明：*P*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。推荐使用```APERCENTILE```函数，该函数性能远胜于```PERCENTILE```函数

    ```mysql
    taos> SELECT APERCENTILE(current, 20) FROM d1001;
    apercentile(current, 20)  |
    ============================
                10.300000191 |
    Query OK, 1 row(s) in set (0.000645s)
    ```
    
- **LAST_ROW**
    ```mysql
    SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
    ```
    功能说明：返回表/超级表的最后一条记录。

    返回结果数据类型：同应用的字段。

    应用字段：所有字段。

    适用于：**表、超级表**。

    说明：与LAST函数不同，LAST_ROW不支持时间范围限制，强制返回最后一条记录。

    限制：LAST_ROW()不能与INTERVAL一起使用。

    示例：
    ```mysql
    taos> SELECT LAST_ROW(current) FROM meters;
    last_row(current)   |
    =======================
                12.30000 |
    Query OK, 1 row(s) in set (0.001238s)

    taos> SELECT LAST_ROW(current) FROM d1002;
    last_row(current)   |
    =======================
                10.30000 |
    Query OK, 1 row(s) in set (0.001042s)
    ```

- **INTERP**
    ```mysql
    SELECT INTERP(field_name) FROM { tb_name | stb_name } WHERE ts='timestamp' [FILL ({ VALUE | PREV | NULL | LINEAR})];
    ```
    功能说明：返回表/超级表的指定时间截面、指定字段的记录。

    返回结果数据类型：同应用的字段。

    应用字段：所有字段。

    适用于：**表、超级表**。

    说明：（从 2.0.15.0 版本开始新增此函数）INTERP 必须指定时间断面，如果该时间断面不存在直接对应的数据，那么会根据 FILL 参数的设定进行插值。其中，条件语句里面可以附带更多的筛选条件，例如标签、tbname。

    限制：INTERP 目前不支持 FILL(NEXT)。

    示例：
    ```mysql
    taos> select interp(*) from meters where ts='2017-7-14 10:42:00.005' fill(prev);
           interp(ts)        | interp(f1)  | interp(f2)  | interp(f3)  |
    ====================================================================
     2017-07-14 10:42:00.005 |           5 |           9 |           6 |
    Query OK, 1 row(s) in set (0.002912s)
    
    taos> select interp(*) from meters where tbname in ('t1') and ts='2017-7-14 10:42:00.005' fill(prev);
           interp(ts)        | interp(f1)  | interp(f2)  | interp(f3)  |
    ====================================================================
     2017-07-14 10:42:00.005 |           5 |           6 |           7 |
    Query OK, 1 row(s) in set (0.002005s)
    ```

### 计算函数

- **DIFF**
    ```mysql
    SELECT DIFF(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的值与前一行对应值的差。

    返回结果数据类型：同应用字段。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、（超级表）**。

    说明：输出结果行数是范围内总行数减一，第一行没有结果输出。从 2.1.3.0 版本开始，DIFF 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

    示例：
    ```mysql
    taos> SELECT DIFF(current) FROM d1001;
            ts            |    diff(current)     |
    =================================================
    2018-10-03 14:38:15.000 |              2.30000 |
    2018-10-03 14:38:16.800 |             -0.30000 |
    Query OK, 2 row(s) in set (0.001162s)
    ```

- **DERIVATIVE**
    ```mysql
    SELECT DERIVATIVE(field_name, time_interval, ignore_negative) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列数值的单位变化率。其中单位时间区间的长度可以通过 time_interval 参数指定，最小可以是 1 秒（1s）；ignore_negative 参数的值可以是 0 或 1，为 1 时表示忽略负值。

    返回结果数据类型：双精度浮点数。

    应用字段：不能应用在 timestamp、binary、nchar、bool 类型字段。

    适用于：**表、（超级表）**。

    说明：（从 2.1.3.0 版本开始新增此函数）输出结果行数是范围内总行数减一，第一行没有结果输出。DERIVATIVE 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

- **SPREAD**
    ```mysql
    SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的最大值和最小值之差。

    返回结果数据类型：双精度浮点数。

    应用字段：不能应用在binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    说明：可用于TIMESTAMP字段，此时表示记录的时间覆盖范围。

    示例：
    ```mysql
    taos> SELECT SPREAD(voltage) FROM meters;
        spread(voltage)      |
    ============================
                5.000000000 |
    Query OK, 1 row(s) in set (0.001792s)

    taos> SELECT SPREAD(voltage) FROM d1001;
        spread(voltage)      |
    ============================
                3.000000000 |
    Query OK, 1 row(s) in set (0.000836s)
    ```

- **四则运算**

    ```mysql
    SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause];
    ```
    功能说明：统计表/超级表中某列或多列间的值加、减、乘、除、取余计算结果。

    返回结果数据类型：双精度浮点数。

    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    适用于：**表、超级表**。

    说明：

    1）支持两列或多列之间进行计算，可使用括号控制计算优先级；

    2）NULL字段不参与计算，如果参与计算的某行中包含NULL，该行的计算结果为NULL。

    ```mysql
    taos> SELECT current + voltage * phase FROM d1001;
    (current+(voltage*phase)) |
    ============================
                78.190000713 |
                84.540003240 |
                80.810000718 |
    Query OK, 3 row(s) in set (0.001046s)
    ```

## <a class="anchor" id="aggregation"></a>按窗口切分聚合

TDengine 支持按时间段等窗口切分方式进行聚合结果查询，比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值。这类聚合适合于降维（down sample）操作，语法如下：

```mysql
SELECT function_list FROM tb_name
  [WHERE where_condition]
  [SESSION(ts_col, tol_val)]
  [STATE_WINDOW(col)]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]

SELECT function_list FROM stb_name
  [WHERE where_condition]
  [SESSION(ts_col, tol_val)]
  [STATE_WINDOW(col)]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]
  [GROUP BY tags]
```

- 在聚合查询中，function_list 位置允许使用聚合和选择函数，并要求每个函数仅输出单个结果（例如：COUNT、AVG、SUM、STDDEV、LEASTSQUARES、PERCENTILE、MIN、MAX、FIRST、LAST），而不能使用具有多行输出结果的函数（例如：TOP、BOTTOM、DIFF 以及四则运算）。
- 查询过滤、聚合等操作按照每个切分窗口为独立的单位执行。聚合查询目前支持三种窗口的划分方式：
  1. 时间窗口：聚合时间段的窗口宽度由关键词 INTERVAL 指定，最短时间间隔 10 毫秒（10a）；并且支持偏移 offset（偏移必须小于间隔），也即时间窗口划分与“UTC 时刻 0”相比的偏移量。SLIDING 语句用于指定聚合时间段的前向增量，也即每次窗口向前滑动的时长。当 SLIDING 与 INTERVAL 取值相等的时候，滑动窗口即为翻转窗口。
    * 从 2.1.5.0 版本开始，INTERVAL 语句允许的最短时间间隔调整为 1 微秒（1u），当然如果所查询的 DATABASE 的时间精度设置为毫秒级，那么允许的最短时间间隔为 1 毫秒（1a）。
    * **注意：**用到 INTERVAL 语句时，除非极特殊的情况，都要求把客户端和服务端的 taos.cfg 配置文件中的 timezone 参数配置为相同的取值，以避免时间处理函数频繁进行跨时区转换而导致的严重性能影响。
  2. 状态窗口：使用整数或布尔值来标识产生记录时设备的状态量，产生的记录如果具有相同的状态量取值则归属于同一个状态窗口，数值改变后该窗口关闭。状态量所对应的列作为 STATE_WINDOW 语句的参数来指定。
  3. 会话窗口：时间戳所在的列由 SESSION 语句的 ts_col 参数指定，会话窗口根据相邻两条记录的时间戳差值来确定是否属于同一个会话——如果时间戳差异在 tol_val 以内，则认为记录仍属于同一个窗口；如果时间变化超过 tol_val，则自动开启下一个窗口。
- WHERE 语句可以指定查询的起止时间和其他过滤条件。
- FILL 语句指定某一窗口区间数据缺失的情况下的填充模式。填充模式包括以下几种：
  1. 不进行填充：NONE（默认填充模式）。
  2. VALUE 填充：固定值填充，此时需要指定填充的数值。例如：FILL(VALUE, 1.23)。
  3. PREV 填充：使用前一个非 NULL 值填充数据。例如：FILL(PREV)。
  4. NULL 填充：使用 NULL 填充数据。例如：FILL(NULL)。
  5. LINEAR 填充：根据前后距离最近的非 NULL 值做线性插值填充。例如：FILL(LINEAR)。
  6. NEXT 填充：使用下一个非 NULL 值填充数据。例如：FILL(NEXT)。

说明：
  1. 使用 FILL 语句的时候可能生成大量的填充输出，务必指定查询的时间区间。针对每次查询，系统可返回不超过 1 千万条具有插值的结果。
  2. 在时间维度聚合中，返回的结果中时间序列严格单调递增。
  3. 如果查询对象是超级表，则聚合函数会作用于该超级表下满足值过滤条件的所有表的数据。如果查询中没有使用 GROUP BY 语句，则返回的结果按照时间序列严格单调递增；如果查询中使用了 GROUP BY 语句分组，则返回结果中每个 GROUP 内不按照时间序列严格单调递增。

时间聚合也常被用于连续查询场景，可以参考文档 [连续查询(Continuous Query)](https://www.taosdata.com/cn/documentation/advanced-features#continuous-query)。

**示例**： 智能电表的建表语句如下：

```mysql
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

针对智能电表采集的数据，以 10 分钟为一个阶段，计算过去 24 小时的电流数据的平均值、最大值、电流的中位数、以及随着时间变化的电流走势拟合直线。如果没有计算值，用前一个非 NULL 值填充。使用的查询语句如下：

```mysql
SELECT AVG(current), MAX(current), LEASTSQUARES(current, start_val, step_val), PERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d
  INTERVAL(10m)
  FILL(PREV);
```

## <a class="anchor" id="limitation"></a>TAOS SQL 边界限制

- 数据库名最大长度为 32
- 表名最大长度为 192，每行数据最大长度 16k 个字符（注意：数据行内每个 BINARY/NCHAR 类型的列还会额外占用 2 个字节的存储位置）
- 列名最大长度为 64，最多允许 1024 列，最少需要 2 列，第一列必须是时间戳
- 标签名最大长度为 64，最多允许 128 个，可以 1 个，一个表中标签值的总长度不超过 16k 个字符
- SQL 语句最大长度 65480 个字符，但可通过系统配置参数 maxSQLLength 修改，最长可配置为 1M
- SELECT 语句的查询结果，最多允许返回 1024 列（语句中的函数调用可能也会占用一些列空间），超限时需要显式指定较少的返回数据列，以避免语句执行报错。
- 库的数目，超级表的数目、表的数目，系统不做限制，仅受系统资源限制

##  TAOS SQL其他约定

**GROUP BY的限制**

TAOS  SQL支持对标签、TBNAME进行GROUP BY操作，也支持普通列进行GROUP BY，前提是：仅限一列且该列的唯一值小于10万个。

**JOIN操作的限制**

TAOS SQL支持表之间按主键时间戳来join两张表的列，暂不支持两个表之间聚合后的四则运算。

**IS NOT NULL与不为空的表达式适用范围**

IS NOT NULL支持所有类型的列。不为空的表达式为 <>""，仅对非数值类型的列适用。


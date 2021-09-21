## 样例数据导入

该工具可以根据用户提供的 `json` 或 `csv` 格式样例数据文件快速导入 `TDengine`，目前仅能在 Linux 上运行。

为了体验写入和查询性能，可以对样例数据进行横向、纵向扩展。横向扩展是指将一个表（监测点）的数据克隆到多张表，纵向扩展是指将样例数据中的一段时间范围内的数据在时间轴上复制。该工具还支持历史数据导入至当前时间后持续导入，这样可以测试插入和查询并行进行的场景，以模拟真实环境。

## 下载安装

### 下载可执行文件

由于该工具使用 go 语言开发，为了方便使用，项目中已经提供了编译好的可执行文件 `bin/taosimport`。通过 `git clone https://github.com/taosdata/TDengine.git` 命令或者直接下载 `ZIP` 文件解压进入样例导入程序目录 `cd importSampleData`，执行 `bin/taosimport`。

### go 源码编译

由于该工具使用 go 语言开发，编译之前需要先安装 go，具体请参考 [Getting Started][2]，而且需要安装 TDengine 的 Go Connector, 具体请参考[TDengine 连接器文档][3]。安装完成之后，执行以下命令即可编译成可执行文件 `bin/taosimport`。
```shell
go get https://github.com/taosdata/TDengine/importSampleData
cd $GOPATH/src/github.com/taosdata/TDengine/importSampleData
go build -o bin/taosimport app/main.go
```

> 注：由于目前 TDengine 的 go connector 只支持 linux 环境，所以该工具暂时只能在 linux 系统中运行。
> 如果 go get 失败可以下载之后复制 `github.com/taosdata/TDengine/importSampleData` 文件夹到 $GOPATH 的 src 目录下再执行 `go build -o bin/taosimport app/main.go`。

## 使用

### 快速体验

执行命令 `bin/taosimport` 会根据默认配置执行以下操作：
1. 创建数据库

   自动创建名称为 `test_yyyyMMdd` 的数据库。
   
2. 创建超级表

   根据配置文件 `config/cfg.toml` 中指定的 `sensor_info` 场景信息创建相应的超级表。
   > 建表语句： create table s_sensor_info(ts timestamp, temperature int, humidity float) tags(location binary(20), color binary(16), devgroup int);

3. 自动建立子表并插入数据

   根据配置文件 `config/cfg.toml` 中 `sensor_info` 场景指定的 `data/sensor_info.csv` 样例数据进行横向扩展 `100` 倍(可通过 hnum 参数指定)，即自动创建 `10*100=1000` 张子表(默认样例数据中有 10 张子表，每张表 100 条数据)，启动 `10` 个线程（可通过 thread 参数指定）对每张子表循环导入 `1000` 次(可通过 vnum 参数指定)。

进入 `taos shell`，可运行如下查询验证：

* 查询记录数

    ```shell
    taos> use test_yyyyMMdd;
    taos> select count(*) from s_sensor_info;
    ```
* 查询各个分组的记录数

    ```shell
    taos> select count(*) from s_sensor_info group by devgroup;
    ```
* 按 1h 间隔查询各聚合指标

    ```shell
    taos> select count(temperature), sum(temperature), avg(temperature) from s_sensor_info interval(1h);
    ```
* 查询指定位置最新上传指标

    ```shell
    taos> select last(*) from s_sensor_info where location = 'beijing';
    ```
> 更多查询及函数使用请参考 [数据查询][4]

### 详细使用说明

执行命令 `bin/taosimport -h` 可以查看详细参数使用说明：

* -cfg string
    
    导入配置文件路径，包含样例数据文件相关描述及对应 TDengine 配置信息。默认使用 `config/cfg.toml`。
    
* -cases string

    需要导入的场景名称，该名称可从 -cfg 指定的配置文件中 `[usecase]` 查看，可同时导入多个场景，中间使用逗号分隔，如：`sensor_info,camera_detection`，默认为 `sensor_info`。
    
* -hnum int

    需要将样例数据进行横向扩展的倍数，假设原有样例数据包含 1 张子表 `t_0` 数据，指定 hnum 为 2 时会根据原有表名创建 `t_0、t_1` 两张子表。默认为 100。
    
* -vnum int

    需要将样例数据进行纵向扩展的次数，如果设置为 0 代表将历史数据导入至当前时间后持续按照指定间隔导入。默认为 1000，表示将样例数据在时间轴上纵向复制1000 次。

* -delay int
    
    当 vnum 设置为 0 时持续导入的时间间隔，默认为所有场景中最小记录间隔时间的一半，单位 ms。

* -tick int

    打印统计信息的时间间隔，默认 2000 ms。

* -save int

    是否保存统计信息到 tdengine 的 statistic 表中，1 是，0 否， 默认 0。

* -savetb string

    当 save 为 1 时保存统计信息的表名， 默认 statistic。

* -auto int
    
    是否自动生成样例数据中的主键时间戳，1 是，0 否， 默认 0。
    
* -start string

    导入的记录开始时间，格式为 `"yyyy-MM-dd HH:mm:ss.SSS"`，不设置会使用样例数据中最小时间，设置后会忽略样例数据中的主键时间，会按照指定的 start 进行导入。如果 auto 为 1，则必须设置 start，默认为空。
    
* -interval int

    导入的记录时间间隔，该设置只会在指定 `auto=1` 之后生效，否则会根据样例数据自动计算间隔时间。单位为毫秒，默认 1000。
  
* -thread int
 
    执行导入数据的线程数目，默认为 10。
  
* -batch int
    
    执行导入数据时的批量大小，默认为 100。批量是指一次写操作时，包含多少条记录。
 
* -host string

    导入的 TDengine 服务器 IP，默认为 127.0.0.1。
  
* -port int

    导入的 TDengine 服务器端口，默认为 6030。
  
* -user string

    导入的 TDengine 用户名，默认为 root。
  
* -password string

    导入的 TDengine 用户密码，默认为 taosdata。

* -dropdb int
    
    导入数据之前是否删除数据库，1 是，0 否， 默认 0。

* -db string

    导入的 TDengine 数据库名称，默认为 test_yyyyMMdd。
  
* -dbparam string

    当指定的数据库不存在时，自动创建数据库时可选项配置参数，如 `days 10 cache 16000 ablocks 4`，默认为空。

### 常见使用示例

* `bin/taosimport -cfg config/cfg.toml -cases sensor_info,camera_detection -hnum 1 -vnum 10`

    执行上述命令后会将 sensor_info、camera_detection 两个场景的数据各导入 10 次。

* `bin/taosimport -cfg config/cfg.toml -cases sensor_info -hnum 2 -vnum 0 -start "2019-12-12 00:00:00.000" -interval 5000`

    执行上述命令后会将 sensor_info 场景的数据横向扩展2倍从指定时间 `2019-12-12 00:00:00.000` 开始且记录间隔时间为 5000 毫秒开始导入，导入至当前时间后会自动持续导入。

### config/cfg.toml 配置文件说明
    
``` toml
# 传感器场景
[sensor_info]                               # 场景名称
format = "csv"                              # 样例数据文件格式，可以是 json 或 csv，具体字段应至少包含 subTableName、tags、fields 指定的字段。
filePath = "data/sensor_info.csv"           # 样例数据文件路径，程序会循环使用该文件数据
separator = ","                             # csv 样例文件中字段分隔符，默认逗号

stname = "sensor_info"                      # 超级表名称
subTableName = "devid"                      # 使用样例数据中指定字段当作子表名称一部分，子表名称格式为 t_subTableName_stname，扩展表名为 t_subTableName_stname_i。
timestamp = "ts"                            # 使用 fields 中哪个字段当作主键，类型必须为 timestamp
timestampType="millisecond"                 # 样例数据中主键时间字段是 millisecond 还是 dateTime 格式
#timestampTypeFormat = "2006-01-02 15:04:05.000"  # 主键日期时间格式，timestampType 为 dateTime 时需要指定
tags = [
    # 标签列表，name 为标签名称，type 为标签类型
    { name = "location",             type = "binary(20)" },
    { name = "color",             type = "binary(16)" },
    { name = "devgroup",           type = "int" },
]

fields = [
    # 字段列表，name 为字段名称，type 为字段类型
    { name = "ts",                  type = "timestamp" },
    { name = "temperature",         type = "int" },
    { name = "humidity",            type = "float" },
]

# 摄像头检测场景
[camera_detection]                          # 场景名称
format = "json"                             # 样例数据文件格式，可以是 json 或 csv，具体字段应至少包含 subTableName、tags、fields 指定的字段。
filePath = "data/camera_detection.json"     # 样例数据文件路径，程序会循环使用该文件数据
#separator = ","                            # csv 样例文件中字段分隔符，默认逗号, 如果是 json 文件可以不用配置
 
stname = "camera_detection"                 # 超级表名称
subTableName = "sensor_id"                  # 使用样例数据中指定字段当作子表名称一部分，子表名称格式为 t_subTableName_stname，扩展表名为 t_subTableName_stname_i。
timestamp = "ts"                            # 使用 fields 中哪个字段当作主键，类型必须为 timestamp
timestampType="dateTime"                    # 样例数据中主键时间字段是 millisecond 还是 dateTime 格式
timestampTypeFormat = "2006-01-02 15:04:05.000"  # 主键日期时间格式，timestampType 为 dateTime 时需要指定
tags = [
    # 标签列表，name 为标签名称，type 为标签类型
    { name = "home_id",           type = "binary(30)" },
    { name = "object_type",       type = "int" },
    { name = "object_kind",       type = "binary(20)" },
]

fields = [
    # 字段列表，name 为字段名称，type 为字段类型
    { name = "ts",                type = "timestamp" },
    { name = "states",               type = "tinyint" },
    { name = "battery_voltage",   type = "float" },
]

# other cases

```

### 样例数据格式说明

#### json

当配置文件 `config/cfg.toml` 中各场景的 format="json" 时，样例数据文件需要提供 tags 和 fields 字段列表中的字段值。样例数据格式如下：

```json
{"home_id": "603", "sensor_id": "s100", "ts": "2019-01-01 00:00:00.000", "object_type": 1, "object_kind": "night", "battery_voltage": 0.8, "states": 1}
{"home_id": "604", "sensor_id": "s200", "ts": "2019-01-01 00:00:00.000", "object_type": 2, "object_kind": "day", "battery_voltage": 0.6, "states": 0}
```

#### csv

当配置文件 `config/cfg.toml` 中各场景的 format="csv" 时，样例数据文件需要提供表头和对应的数据，其中字段分隔符由使用场景中 `separator` 指定，默认逗号。具体格式如下：

```csv
devid,location,color,devgroup,ts,temperature,humidity
0, beijing, white, 0, 1575129600000, 16, 19.405091
0, beijing, white, 0, 1575129601000, 22, 14.377142
```



[1]: https://github.com/taosdata/TDengine
[2]: https://golang.org/doc/install
[3]: https://www.taosdata.com/cn/documentation/connector/#Go-Connector
[4]: https://www.taosdata.com/cn/documentation/taos-sql/#%E6%95%B0%E6%8D%AE%E6%9F%A5%E8%AF%A2
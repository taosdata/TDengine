# MQTT Advanced Options 连接器实现

新加配置项样例如下
```go
[dump]
enable = true
path = "./tmp"
keep = 7
```


| 参数 | 类型 | 说明 | 对应 taosx 配置 |
| --- | --- | --- | --- |
| enable | bool | 是否启用保存原始数据 | keep_raw_data |
| path | string | 保存文件路径，文件名格式mqtt.dump.%Y%m%d%H%M | keep_raw_data_dir |
| keep | int | 最大保留天数 | keep_raw_data_days |

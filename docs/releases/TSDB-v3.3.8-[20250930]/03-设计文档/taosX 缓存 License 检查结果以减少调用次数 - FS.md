# taosX 缓存 License 检查结果以减少调用次数 - FS

## 1. 背景

taosX 覆盖率测试频繁出现任务创建超时错误，导致近期 taosx 覆盖率测试全部失败，分析情况见[taosX 覆盖率 CI 多次失败分析](https://taosdata.feishu.cn/wiki/WQ7xwQuZgij1s9kY1ljcvSH6nFc)。License 检查是每次任务创建都会进行的检查，从分析情况看，是导致超时的重要原因之一。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/09/05 | 0.1 | Linhe Huo | 初稿 |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

## 4. 行为说明

此优化不影响用户行为。

### 4.1 内存缓存

优化前后用时对比：
```plaintext {wrap}
[taos://localhost:6030/test]: cache miss cost 22.940006ms
[taos://localhost:6030/test]: cache hit cost 110.468µs
[csv://localhost:6030/test]: cache miss cost 11.799971ms
[csv://localhost:6030/test]: cache hit cost 112.294µs
[tmq://localhost:6030/test]: cache miss cost 12.958811ms
[tmq://localhost:6030/test]: cache hit cost 123.595µs
[influxdb://localhost:6030/test]: cache miss cost 7.099579ms
[influxdb://localhost:6030/test]: cache hit cost 112.204µs
[influxdb://localhost:6030/test]: cache miss cost 5.878254ms
[influxdb://localhost:6030/test]: cache hit cost 75.093µs
[pi://localhost:6030/test]: cache miss cost 5.216453ms
[pi://localhost:6030/test]: cache hit cost 118.358µs
[pibackfill://localhost:6030/test]: cache miss cost 5.62593ms
[pibackfill://localhost:6030/test]: cache hit cost 92.381µs
[kafka://localhost:6030/test]: cache miss cost 4.6761ms
[kafka://localhost:6030/test]: cache hit cost 123.876µs
[mqtt://localhost:6030/test]: cache miss cost 7.629643ms
[mqtt://localhost:6030/test]: cache hit cost 93.308µs
[mongodb://localhost:6030/test]: cache miss cost 4.450486ms
[mongodb://localhost:6030/test]: cache hit cost 84.927µs
[oracle://localhost:6030/test]: cache miss cost 5.286975ms
[oracle://localhost:6030/test]: cache hit cost 106.229µs
[mysql://localhost:6030/test]: cache miss cost 5.373573ms
[mysql://localhost:6030/test]: cache hit cost 81.41µs
[mssql://localhost:6030/test]: cache miss cost 4.34302ms
[mssql://localhost:6030/test]: cache hit cost 86.559µs
[opentsdb://localhost:6030/test]: cache miss cost 4.894168ms
[opentsdb://localhost:6030/test]: cache hit cost 89.077µs
```

### 4.2 自定义缓存超时时间

使用环境变量自定义缓存超时时间：TAOSX_LICENSE_CACHE_TTL，默认 1 小时（使用数字，单位为秒）。
如需修改，Linux 下使用 systemd 方式启动时，需添加到环境变量文件 `/etc/default/taosx` 中：
```shell {wrap}
tee -a /etc/default/taosx <<EOF
TAOSX_LICENSE_CACHE_TTL=3600
EOF
```

Windows 下需添加到系统环境变量中。

## 5. 性能

- 无

## 6. 兼容性

- 无

## 7. 运维

- 无

## 8. 使用场景

- 无

## 9. 可观测性

- 无

## 10. 安装和卸载

- 无特殊要求

## 11. 文档

## 12. 参考文档

## 13. 附录

- 无。

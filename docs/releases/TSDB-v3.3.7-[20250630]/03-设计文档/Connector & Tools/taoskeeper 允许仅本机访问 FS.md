# taoskeeper 允许仅本机访问 FS

## 1. 背景

taoskeeper 当前监听所有地址，且 API 无安全认证，存在一定的安全风险。因此需要支持能制定监听地址的方式，先提供配置项，允许仅监听 127.0.0.1 。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 备注 |
| --- | --- | --- | --- |
| 45808 | 0.1 | 佘彦杰 | 初稿 |

## 3. 定义

1. **taoskeeper：**taosKeeper 是 TDengine 3.0 版本监控指标的导出工具，通过简单的几项配置即可获取 TDengine 的运行状态。taosKeeper 使用 TDengine RESTful 接口，所以不需要安装 TDengine 客户端即可使用。
2. **IPv4：**是32位地址（如 `192.168.1.1`），地址空间有限。
3. **IPv6** 是128位地址（如 `2001:db8::1`），解决IPv4地址耗尽问题，并内置更高效的路由和安全特性。
4. **taosAdapter：**taosAdapter 是一个 TDengine 的配套工具，是 TDengine 集群和应用程序之间的桥梁和适配器。TDengine 的各语言连接器通过 WebSocket 接口与 TDengine 进行通信，因此必须安装 taosAdapter。

## 4. 行为说明

### 4.1 配置文件修改

新增配置项 host
```yaml

## 5. Listening host, supports IPv4/Ipv6, default is ""

host = ""
```

支持 Ipv4，Ipv6，已经主机名或域名形式。下面配置都是合法的
```yaml
host = "127.0.0.1"
host = "2001:db8::1"
host = "fe80::a6bb:6dff:fed9:9817"
host = "fe80::a6bb:6dff:fed9:9817%eno1"
host = "example.com"
```

对于非法的配置，在监听地址时会报错，配置解析不做处理。

## 6. 测试用例

[taoskeeper 允许仅本机访问 TS](https://taosdata.feishu.cn/wiki/BWPlwTcfCi52xwkUT6NcAGzOnie)

## 7. 使用方式

配置文件

## 8. 性能

不涉及

## 9. 兼容性

新增参数控制，无兼容性问题。

## 10. 运维

无。

## 11. 使用场景

1. 用户需要设置单独监听地址，提高安全性的时候。

## 12. 约束和限制

无

## 13. 常见错误和排查

监听失败会报错，taoskeeper 启动失败，打印错误信息。

## 14. 安装和卸载

不涉及

## 15. 文档

需要提供用户手册

## 16. 参考文档

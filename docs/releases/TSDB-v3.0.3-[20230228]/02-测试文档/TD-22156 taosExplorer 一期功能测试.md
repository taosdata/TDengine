# TD-22156：taosExplorer 一期功能测试

## 1. 测试结论：

taosExplorer 一期的功能基本完成，需要优化的地方还有一项：
TD-23073

## 2. 测试概述：

一期主要需要实现的功能如下：

TD-22601


TD-22602


TD-22603


TD-22604


TD-22605


TD-22606


TD-22607

## 3. 测试环境：

taosExplorer 服务器地址： http://192.168.0.201:6060
阿里云服务器地址：http://39.107.123.129:6060

## 4. 测试步骤

1. 安装最新的企业版 3.0.3.0
2. 配置 /etc/taos/explorer.toml
```xml
listen = "0.0.0.0:6060"
log_level = "info"
x_api = "http://localhost:6050"
cluster = "http://http://39.107.123.129:6041"
```

1. 依次启动 taosd, taosadapter, taos-explorer, taosx
2. 在浏览器地址栏输入 http://39.107.123.129:6060 即可开始测试

## 5. 测试结果

发现的问题列表：

TD-23169


TD-23073


TD-23048


TD-22939


TD-22908


TD-22897


TD-22869


TD-22850


TD-22808


TD-22802

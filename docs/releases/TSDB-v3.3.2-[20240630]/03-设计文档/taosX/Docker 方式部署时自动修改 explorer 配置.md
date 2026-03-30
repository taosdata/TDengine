# Docker 方式部署时自动修改 explorer 配置

## 1. 背景

explorer 页面中有一些需要显示 taosadapter 地址的地方，例如`概览`、`编程`与`工具` 等，这个地址是根据配置文件中的 `cluster` 生成。如果 taosd/taosadapter/explorer 是在 docker 中部署，那么用户看到的地址只是容器内部的地址，在容器外无法正常使用，会误导用户，所以需要针对 docker 部署场景做优化。相关 jira 如下：

TD-30280

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/06/26 | 0.1 | @张元湃 |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

无。

## 4. 行为说明

1. explorer.toml 文件中新增仅用于前端展示的配置项 `cluster_adapter=``http://localhost:6041/`
   - 此参数需要配置为 `cluster` 中的 `taosadapter` 的可正常访问地址
   - 如果参数未配置，则前端展示时使用 `cluster` 配置
2. 创建 docker 容器时，命令行参数增加以下参数：
```shell

## 5. 其中 ip 是宿主机可被访问的 ip 地址，port 是 docker 容器内的 taosadapter 映射到宿主机的端口

docker run -e "EXTERNAL_ADAPTER_ADDR=http(s)://ip:port" 7a0beafb39e9
```

## 6. 性能

对性能无影响。

## 7. 兼容性

不涉及兼容性。

## 8. 运维

用户创建 docker 容器时需要增加命令行参数，创建 docker 容器后需要检查配置文件内容是否符合预期。

## 9. 使用场景

使用 docker 容器部署 taosd/taosadapter/explorer 时适用。

## 10. 约束和限制

1. 命令行中新增的参数值必须按照实际情况填写，否则页面中 taosadapter 地址显示错误
2. 如果设置了 /data/taosx/config/explorer.toml 文件的映射，可能需要手动修改文件中 `cluster_adapter` 的内容

## 11. 常见错误和排查

1. 容器内部配置文件 /etc/taos/explorer.toml 没有按照预期修改成功：可以检查创建 docker 容器的命令行中是否增加了参数`-e "EXTERNAL_ADAPTER_ADDR=``http://ip``:p``ort"`，如果增加了，则再检查容器内部是否有环境变量 `EXTERNAL_ADAPTER_ADDR` 的值
2. 容器内部配置文件 /etc/taos/explorer.toml 已修改，但页面中不生效：可以检查创建 docker 容器的命令行中是否设置了 /data/taosx/config/explorer.toml 文件的映射，如果设置了，需要手动修改此文件内容

## 12. 可观测性

相关页面中显示的 taosadapter 地址应该与创建 docker 容器时配置的 `EXTERNAL_ADAPTER_ADDR` 地址一致。

## 13. 安装和卸载

无。

## 14. 文档

修改官网文档。

## 15. 参考文档

无。

## 16. 附录

### 16.1 explorer 前端实现方案

前端获取 explorer.toml 中的 `cluster_adapter` 参数，用于拼接各页面中的 taosadapter 地址，如果参数为空则使用 `cluster` 参数。

### 16.2 docker 部署实现方案

1. 修改 Dockerfile 文件，增加容器内环境变量 `EXTERNAL_ADAPTER_ADDR`
2. 修改 startup.sh 文件，增加根据环境变量 `EXTERNAL_ADAPTER_ADDR` 修改 explorer.toml 文件内容的操作
   - 修改 /etc/taos/explorer.toml 文件
   - 修改 /data/taosx/config/explorer.toml 文件
3. 创建 docker 容器的命令行中增加 `-e "EXTERNAL_ADAPTER_ADDR=http(s)://ip:port"` 参数，创建容器的过程中 startup.sh 脚本会自动修改上述两个配置文件

### 16.3 存在问题

1. 用户需要多配一个参数（docker 部署或 native 连接等特殊情况），用户体验不好
2. docker 部署时需要增加命令行参数，而且这个参数具有迷惑性，用户体验不好
3. 前端页面的显示（以及后续使用）依赖用户自定义输入，系统不能验证它的正确性，会引发运维问题

# taos-explorer

taos-explorer web server

## 参数说明

1. 命令行参数 8082 "info" ，8082 为端口（数字），”info“ 为日志级别
2. 环境变量 TAOS_EXPLORER_PORT 设置端口，TAOS_EXPLORER_LOG_LEVEL 设置日志级别
3. 配置文件名称 config.toml 与执行文件在同一目录，内容如下:

```toml
port = 8082
log_level = "info"
```

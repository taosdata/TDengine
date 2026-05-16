# JMeter集成使用指南

本目录包含了在taos-test-framework中集成JMeter进行性能测试的示例和说明。

## 功能特性

### 1. 变量替换功能
- 支持多种变量格式：`${variable}`、`#{variable}`、`@{variable}`、`{{variable}}`
- 自动在JMX文件中替换配置的变量值
- 支持全局变量和文件特定变量

### 2. 灵活的配置方式
- YAML配置文件驱动
- 支持单文件和多文件执行
- 支持在线和离线模式
- 自动安装Java和JDBC驱动

### 3. 智能路径解析
- 支持相对路径和绝对路径
- 自动在多个目录中查找JMX文件：
  - 用例文件同目录
  - TestNG/env目录
  - TestNG根目录
  - 当前工作目录

### 4. 完整的测试生命周期
- 环境设置和清理
- 结果收集和分析
- 错误处理和日志记录

## 文件说明

### 配置文件
- `jmeter_enhanced.yaml` - JMeter配置示例（支持变量替换）
- `example_database_test.jmx` - TDengine数据库性能测试JMX模板

### 测试用例
- `jmeter_basic_test.py` - JMeter基础功能测试
- `tdengine_performance_test.py` - TDengine数据库性能测试实例

## 快速开始

### 1. 基本使用
```python
from taostest.util.jmeter import quick_run_jmx

# 快速运行JMX文件
result = quick_run_jmx(
    jmx_path="test.jmx",
    variables={
        "host": "192.168.1.100",
        "threads": "50",
        "duration": "300"
    }
)
```

### 2. 高级使用
```python
from taostest.util.jmeter import JMeterTestRunner

with JMeterTestRunner(remote) as runner:
    # 设置环境
    runner.setup_jmeter(config)
    
    # 运行测试
    results = runner.run_multiple_jmx(jmx_configs)
    
    # 收集结果
    runner.collect_results(config, log_dir, results)
```

### 3. YAML配置方式
```python
from taostest.util.jmeter import JMeterTestRunner

with JMeterTestRunner(remote) as runner:
    results = runner.run_from_yaml_config("jmeter_config.yaml")
```

## 配置格式

### YAML配置示例
```yaml
settings:
  - name: jmeter
    fqdn: [node1, node2]
    spec:
      jmeter:
        version: "5.6.3"
        jmx_files:
          - name: "performance_test"
            path: "./performance_test.jmx"
            variables:
              thread_count: 100
              duration: 300
              server_host: "192.168.1.100"
              server_port: 6030
        global_variables:
          environment: "test"
          cluster_name: "taos_cluster"
```

### JMX变量使用
在JMX文件中使用变量：
```xml
<stringProp name="ThreadGroup.num_threads">${thread_count}</stringProp>
<stringProp name="HTTPSampler.domain">${server_host}</stringProp>
<stringProp name="HTTPSampler.port">${server_port}</stringProp>
```

## 部署模式

### 在线模式（默认）
- 自动从官网下载JMeter和JDBC驱动
- 需要网络连接
- 适合开发和测试环境

### 离线模式
- 使用本地安装包
- 不需要网络连接
- 适合生产环境

离线模式配置：
```yaml
spec:
  jmeter:
    offline: true
    local_package_path: "/tmp"
    # 需要准备以下文件：
    # /tmp/apache-jmeter-5.6.3.tgz
    # /tmp/taos-jdbcdriver-3.4.0-dist.jar
```

## 运行示例

### 运行基础测试
```bash
cd TestNG/cases/jmeter_examples
python jmeter_basic_test.py
```

### 运行性能测试
```bash
cd TestNG/cases/jmeter_examples
python tdengine_performance_test.py
```

## 结果分析

测试结果将包含：
- JTL文件（原始测试数据）
- HTML报告（可视化结果）
- JMeter日志文件
- 执行状态和错误信息

结果文件位置：
- 远程服务器：`/tmp/jmeter_tests/`
- 本地收集：指定的log_dir目录

## 错误处理

常见问题和解决方案：

### 1. JMX文件找不到
- 检查文件路径是否正确
- 确认文件在支持的搜索目录中
- 使用绝对路径

### 2. 变量替换失败
- 检查变量名是否匹配
- 确认使用正确的变量格式
- 查看日志中的变量替换信息

### 3. JMeter环境问题
- 检查Java是否正确安装
- 确认JDBC驱动版本兼容性
- 查看JMeter安装日志

### 4. 网络连接问题
- 检查防火墙设置
- 确认数据库连接参数
- 使用离线模式避免网络依赖

## 最佳实践

### 1. 变量管理
- 使用有意义的变量名
- 将环境相关变量提取到全局配置
- 为不同环境准备不同的变量文件

### 2. 测试设计
- 从小规模测试开始，逐步增加负载
- 设置合理的超时和重试参数
- 包含必要的断言验证

### 3. 结果分析
- 关注响应时间、吞吐量和错误率
- 使用HTML报告进行可视化分析
- 保存测试结果用于性能趋势分析

### 4. 环境管理
- 在专用的性能测试环境中运行
- 确保测试环境的稳定性和一致性
- 监控系统资源使用情况

## 扩展开发

如需扩展功能，可以：

1. 继承`JMeterTestRunner`类添加自定义功能
2. 扩展`JMXVariableReplacer`支持更多变量格式
3. 在`tdcase.py`基类中集成JMeter功能
4. 添加更多的结果分析和报告功能

## 注意事项

1. 确保目标服务器有足够的资源运行JMeter
2. 大规模测试前先进行小规模验证
3. 合理设置测试参数避免对生产环境造成影响
4. 定期清理临时文件和测试结果
5. 在集群环境中注意时间同步问题
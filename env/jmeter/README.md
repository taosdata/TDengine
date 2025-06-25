# 项目 proj_aaa - SQL性能测试框架

本项目演示了如何使用JMeter对TDengine数据库进行SQL性能测试，支持遍历SQL文件，逐条执行性能测试，并生成详细的结果分析报告。

## 📁 项目结构

```
proj_aaa/
├── README.md                           # 本说明文件
├── performance_test_template.jmx       # JMeter测试模板（支持变量替换）
├── test_sqls.sql                       # SQL测试用例文件
└── jmeter_config.yaml                  # JMeter配置文件
```

## 🎯 核心功能

### 1. 智能SQL解析
- 自动解析SQL文件，提取每条SQL语句
- 支持注释和多行SQL
- 自动生成描述性的测试名称

### 2. 变量化JMX模板
- 基于真实JMX模板创建的性能测试模板
- 支持数据库连接、线程数、循环次数等参数化配置
- 自动替换SQL语句进行测试

### 3. 多场景测试
- 轻量级负载测试 (10线程)
- 中等负载测试 (50线程)
- 高负载测试 (100线程)
- 压力测试 (200线程)

### 4. 结果分析与报告
- 自动生成JSON和CSV格式的结果汇总
- 性能统计分析（平均时间、最大时间、成功率等）
- 生成HTML可视化报告
- 按时间戳创建结果目录

## 🚀 使用方法

### 1. 配置数据库连接
编辑 `jmeter_config.yaml` 文件：
```yaml
global_variables:
  db_host: "192.168.1.55"     # 数据库主机
  db_port: "6041"             # 数据库端口
  username: "root"            # 用户名
  password: "taosdata"        # 密码
```

### 2. 准备SQL测试用例
编辑 `test_sqls.sql` 文件，添加你的SQL语句：
```sql
-- 查询测试
SELECT tagid, last(v) as value FROM yjb2c.type_float WHERE tagid LIKE '%5001' GROUP BY tagid;

-- 聚合测试  
SELECT tagid, AVG(v), MAX(v), MIN(v) FROM yjb2c.type_float GROUP BY tagid LIMIT 100;
```

### 3. 运行性能测试
```python
from taostest.util.jmeter_sql_runner import JMeterSQLPerformanceRunner

with JMeterSQLPerformanceRunner(remote) as runner:
    results = runner.run_sql_performance_tests(
        config_path="proj_aaa/jmeter_config.yaml",
        scenario="medium_load"
    )
```

### 4. 使用测试用例
```bash
cd TestNG/cases/jmeter_examples
python sql_performance_test.py
```

## 📊 测试场景说明

| 场景 | 线程数 | 循环次数 | 爬坡时间 | 适用场景 |
|------|--------|----------|----------|----------|
| light_load | 10 | 20 | 2s | 功能验证 |
| medium_load | 50 | 64 | 3s | 日常性能测试 |
| heavy_load | 100 | 100 | 5s | 高负载测试 |
| stress_test | 200 | 200 | 10s | 压力极限测试 |

## 📈 结果目录结构

测试结果将保存在 `TestNG/run/` 目录下：

```
run/
└── sql_performance_medium_load/
    └── sql_performance_medium_load_20240101_120000/
        ├── performance_summary.json          # 结果汇总(JSON)
        ├── performance_summary.csv           # 结果汇总(CSV)
        ├── performance_statistics.txt        # 统计报告
        ├── performance_analysis.json         # 分析结果
        ├── performance_analysis_report.html  # HTML可视化报告
        ├── jmeter_results/                   # JMeter原始结果
        │   ├── *.jtl                        # 测试数据文件
        │   ├── *.log                        # JMeter日志
        │   └── *_report/                    # HTML报告目录
        ├── reports/                          # 自定义报告
        └── logs/                             # 测试日志
```

## 🔧 自定义配置

### 修改性能参数
在 `jmeter_config.yaml` 中调整：
```yaml
global_variables:
  thread_count: "50"        # 线程数
  loop_count: "64"          # 循环次数
  query_timeout: "500"      # 查询超时(毫秒)
  pool_max: "64"           # 连接池大小
```

### 设置性能阈值
```yaml
performance_thresholds:
  avg_response_time_ms: 1000    # 平均响应时间阈值
  max_response_time_ms: 5000    # 最大响应时间阈值
  error_rate_percent: 1.0       # 错误率阈值
```

### 添加新的测试场景
```yaml
test_scenarios:
  - name: "custom_test"
    thread_count: "80"
    loop_count: "50"
    ramp_time: "4"
```

## 📝 SQL文件格式

支持的SQL文件格式：
```sql
-- 这是注释，会被忽略

-- 基础查询
SELECT * FROM table1 WHERE condition;

-- 复杂查询（可以多行）
SELECT col1, col2, 
       AVG(col3) as avg_val
FROM table1 
WHERE ts > '2024-01-01'
GROUP BY col1;

-- 聚合查询
SELECT COUNT(*), SUM(value) FROM metrics;
```

注意：
- 每个SQL语句以分号结尾
- 支持单行和多行SQL
- 以 `--` 或 `#` 开头的行被视为注释
- 空行会被自动忽略

## 🔍 结果分析

### 1. 性能指标
- **执行时间**: 每条SQL的执行时间统计
- **成功率**: 成功执行的SQL比例
- **响应时间分布**: 平均、最小、最大、P90、P95、P99
- **吞吐量**: 每秒处理的请求数

### 2. 性能洞察
系统会自动生成性能洞察，包括：
- 🟢 测试执行状态评估
- 📊 响应时间分析
- ❌ 失败模式分析
- ⚠️ 性能瓶颈识别

### 3. 可视化报告
HTML报告提供：
- 整体性能仪表盘
- 详细的SQL测试结果表格
- 性能趋势图表
- 错误分析图表

## 🛠️ 故障排除

### 常见问题

1. **JMeter环境问题**
   ```bash
   # 检查Java环境
   java -version
   
   # 检查JMeter安装
   /opt/apache-jmeter-5.6.3/bin/jmeter --version
   ```

2. **数据库连接问题**
   - 检查网络连通性
   - 验证用户名密码
   - 确认JDBC驱动版本兼容性

3. **SQL执行失败**
   - 检查SQL语法正确性
   - 确认表和字段存在
   - 验证数据权限

4. **性能异常**
   - 检查数据库服务器资源使用
   - 调整JMeter线程数和连接池
   - 优化SQL查询语句

### 日志查看
```bash
# 查看测试日志
tail -f TestNG/run/*/logs/*.log

# 查看JMeter日志
tail -f TestNG/run/*/jmeter_results/*.log
```

## 🔄 集成到CI/CD

可以将性能测试集成到持续集成流程中：

```bash
#!/bin/bash
# 运行性能测试
cd /root/taos-test-framework/TestNG/cases/jmeter_examples
python sql_performance_test.py

# 分析结果
python -m taostest.util.performance_analyzer /root/taos-test-framework/TestNG/run

# 检查性能阈值（返回非0退出码表示性能下降）
python check_performance_regression.py
```

## 📚 扩展开发

### 添加新的分析功能
继承 `PerformanceAnalyzer` 类：
```python
class CustomAnalyzer(PerformanceAnalyzer):
    def custom_analysis(self):
        # 自定义分析逻辑
        pass
```

### 集成其他数据库
修改JMX模板中的JDBC配置：
```xml
<stringProp name="dbUrl">jdbc:mysql://host:port/db</stringProp>
<stringProp name="driver">com.mysql.cj.jdbc.Driver</stringProp>
```

### 添加实时监控
可以集成Prometheus、Grafana等监控工具收集实时性能指标。

## 📞 支持与反馈

如有问题或建议，请通过以下方式联系：
- 查看框架文档
- 提交Issue到项目仓库
- 联系开发团队

---

*本项目基于taos-test-framework，为TDengine数据库性能测试提供完整的解决方案。*
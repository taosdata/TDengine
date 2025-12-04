---
toc_max_heading_level: 4
sidebar_label: R
title: TDengine R Language Connector
---

## 配置

完成了 R 语言环境安装步骤后，您需要进行一些配置，以便 RJDBC 库能够正确连接和访问 TDengine 时序数据库。

1. 在 R 脚本中加载 RJDBC 和其他必要的库：

```r
library(DBI)
library(rJava)
library(RJDBC)
```

2. 设置 JDBC 驱动程序和 JDBC URL：

```r
# 设置JDBC驱动程序路径（根据您实际保存的位置进行修改）
driverPath <- "/path/to/taos-jdbcdriver-X.X.X-dist.jar"

# 设置JDBC URL（根据您的具体环境进行修改）
url <- "<jdbcURL>"
```

3. 加载 JDBC 驱动程序：

```r
# 加载JDBC驱动程序
drv <- JDBC("com.taosdata.jdbc.rs.RestfulDriver", driverPath)
```

4. 创建 TDengine 数据库连接：

```r
# 创建数据库连接
conn <- dbConnect(drv, url)
```

5. 连接成功后，您可以使用 conn 对象进行各种数据库操作，如查询数据、插入数据等。

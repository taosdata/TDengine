# Python 连接器-Test Spec

## 1. **修订记录**

| **日期** | **版本** | **作者** | **备忘** |
| --- | --- | --- | --- |
| 2024-01-15 | 1.0 | 佘彦杰 | 创建 |
| 2025-12-10 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 Python 连接器的所有功能接口是否按照 Function Spec 和 Design Spec 正确实现，包括原生连接、WebSocket 连接、SQL 执行、参数绑定（STMT/STMT2）、无模式写入、数据订阅等核心功能。
2. 性能测试：验证 Python 连接器在数据写入、查询、参数绑定和数据订阅场景下的性能是否满足 Design Spec 中定义的性能指标。
3. 安全性测试：验证 Python 连接器是否满足 Requirement Spec 中定义的安全需求，包括凭证安全存储、传输加密、SQL 注入防护、日志脱敏等。
4. 稳定性测试：验证 Python 连接器在长时间运行、高并发、异常场景下的稳定性和可靠性。
5. 兼容性测试：验证 Python 连接器与不同版本 TDengine、不同 Python 版本、不同操作系统的兼容性。

## 3. **测试范围**

1. 功能测试：
  - 数据库连接：原生连接（Native）、WebSocket 连接（WS/WSS）、REST 连接
  - 数据操作：SQL 执行（DDL/DML/DQL）、数据写入、数据查询、结果集遍历
  - 参数绑定：STMT 参数绑定、STMT2 参数绑定、批量写入、自动建表
  - 无模式写入：InfluxDB 行协议、OpenTSDB telnet 协议、OpenTSDB JSON 协议
  - 数据订阅：TMQ 消费者创建、主题订阅、数据消费、偏移量管理
  - 数据类型：所有 TDengine 支持的数据类型（timestamp、int、float、double、binary、nchar、bool、varbinary、geometry、decimal、blob、json 等）
  - 接口规范：PEP 249 DB-API 2.0 规范、SQLAlchemy 集成
1. 性能测试：
  - 查询性能：单线程拉取 meters 表性能
  - SQL 写入性能：单线程 SQL 写入性能
  - 参数绑定写入性能：单线程 STMT/STMT2 写入性能
  - 数据订阅性能：单线程 TMQ 消费性能
1. 安全性测试：
  - 凭证保护：禁止硬编码凭证、环境变量读取、Token 认证
  - 传输加密：WSS 连接、SSL/TLS 证书验证
  - SQL 注入防护：参数绑定防注入
  - 日志脱敏：密码/Token 脱敏
  - 错误处理：生产环境错误信息简化
1. 稳定性测试：
  - 长时间运行：连续运行 24 小时以上
  - 连接管理：连接池、连接超时、空闲连接回收
  - 异常处理：网络断开重连、无效参数处理
1. 兼容性测试：
  - TDengine 版本：3.3.2.0 及以上版本
  - Python 版本：Python 3.7、3.8、3.9、3.10、3.11、3.12
  - 操作系统：Linux（Ubuntu、CentOS）、macOS、Windows

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

1. 原生连接依赖 TDengine 客户端库（libtaos.so/taos.dll），需要正确安装和配置
2. WebSocket 连接依赖 taosAdapter 服务
3. JSON 类型仅在 tag 中支持
4. 部分功能（如订阅回调）在 TDengine 3.x 版本中已废弃

## 6. **测试环境**

| **系统** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- |
| **Ubuntu 22.04** | TDengine Server + taosAdapter | 16 核 Intel Core i7-10700 @ 2.90GHz | 64GB | 500GB SSD |
| **Ubuntu 22.04** | Python 测试客户端 | 8 核 Intel Core i5 @ 3.00GHz | 32GB | 256GB SSD |
| **Windows 11** | Python 测试客户端 | 8 核 Intel Core i5 @ 3.00GHz | 16GB | 256GB SSD |
| **macOS 14** | Python 测试客户端 | Apple M2 | 16GB | 256GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

#### 7.1.1 **数据库连接测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **原生连接** | 默认连接 | 使用默认参数建立原生连接 | 连接成功，返回 TaosConnection 对象 | test_connection.py::test_default_connect |
| **原生连接** | 指定参数连接 | 指定 host、port、user、password、database 建立连接 | 连接成功，使用指定参数 | test_connect_args.py |
| **原生连接** | 无效数据库连接 | 连接不存在的数据库 | 抛出 ConnectionError 异常 | test_exception.py::test_invalid_dbname |
| **原生连接** | 连接关闭 | 关闭已建立的连接 | 连接正常关闭，资源释放 | test_connection.py |
| **WebSocket 连接** | WS 连接 | 使用 ws:// 协议建立 WebSocket 连接 | 连接成功，返回 Connection 对象 | taos-ws-py/tests/test_connect.py |
| **WebSocket 连接** | WSS 连接 | 使用 wss:// 协议建立加密连接 | 连接成功，SSL/TLS 加密生效 | taos-ws-py/tests/test_connect.py |
| **WebSocket 连接** | Token 认证 | 使用 token 参数连接云服务 | 认证成功，连接建立 | taos-ws-py/tests/test_connect.py |
| **WebSocket 连接** | 多地址连接 | 指定多个服务器地址进行连接 | 自动选择可用地址连接 | test_sqlalchemy.py::test_read_from_sqlalchemy_taosws_failover |
| **REST 连接** | REST 连接 | 通过 taosrest 建立 HTTP 连接 | 连接成功 | test_rest_connection.py |
| **REST 连接** | 带 req_id 查询 | REST 连接执行带 req_id 的查询 | 查询成功，可追踪请求 | test_rest_connection.py::test_query_with_req_id |

#### 7.1.2 **SQL 执行测试**

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| 测试类型 | **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| DDL | 创建数据库 | 执行 CREATE DATABASE 语句 | 数据库创建成功 | test_query.py::test_query |
| DDL | 创建超级表 | 执行 CREATE STABLE 语句 | 超级表创建成功 | test_query.py::test_query |
| DDL | 创建子表 | 执行 CREATE TABLE USING 语句 | 子表创建成功 | test_native_cursor.py::test_cursor |
| DDL | 删除数据库 | 执行 DROP DATABASE 语句 | 数据库删除成功 | test_query.py |
| DML | 插入单条数据 | 执行单条 INSERT 语句 | 数据插入成功，affected_rows=1 | test_query.py |
| DML | 批量插入数据 | 执行多条 INSERT 语句 | 数据批量插入成功 | test_native_many.py |
| DML | 自动建表插入 | 使用 INSERT INTO...USING 语句 | 自动创建子表并插入数据 | test_query.py |
| DQL | 基本查询 | 执行 SELECT 语句 | 返回正确的查询结果 | test_query.py::test_query |
| DQL | 带条件查询 | 执行带 WHERE 条件的查询 | 返回符合条件的结果 | test_sqlalchemy.py::test_stmt2_query |
| DQL | 带 req_id 查询 | 执行带 req_id 的查询 | 查询成功，便于问题追踪 | test_query.py::test_query_with_req_id |
| DQL | 结果集遍历 | 遍历 TaosResult 结果集 | 正确获取所有行数据 | test_query.py |
| DQL | fetch_all_into_dict | 将结果转换为字典列表 | 返回正确的字典格式数据 | test_query.py |

#### 7.1.3 **数据类型测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **基本类型** | TIMESTAMP 类型 | 插入和查询 timestamp 数据 | 数据正确存储和读取 | test_stmt.py |
| **基本类型** | INT/BIGINT 类型 | 插入和查询整数数据 | 数据正确存储和读取 | test_stmt.py::test_stmt_insert |
| **基本类型** | FLOAT/DOUBLE 类型 | 插入和查询浮点数据 | 数据正确存储和读取 | test_stmt.py::test_stmt_insert |
| **基本类型** | BOOL 类型 | 插入和查询布尔数据 | 数据正确存储和读取 | test_stmt.py::test_stmt_insert |
| **字符类型** | BINARY 类型 | 插入和查询 binary 数据 | 数据正确存储和读取 | test_stmt.py::test_stmt_insert |
| **字符类型** | NCHAR 类型 | 插入和查询 nchar 数据（含中文） | 数据正确存储和读取 | test_stmt.py::test_stmt_insert_multi |
| **特殊类型** | VARBINARY 类型 | 插入和查询 varbinary 数据 | 二进制数据正确存储和读取 | test_query.py::test_varbinary |
| **特殊类型** | GEOMETRY 类型 | 插入和查询 geometry 数据 | 几何数据正确存储和读取 | test_query.py::test_varbinary |
| **特殊类型** | DECIMAL 类型 | 插入和查询 decimal 数据 | 高精度数据正确存储和读取 | test_query.py::test_query_decimal |
| **特殊类型** | JSON 类型 | 在 tag 中使用 JSON 类型 | JSON 数据正确存储和读取 | test_query.py::test_query |
| **特殊类型** | BLOB 类型 | 插入和查询 blob 数据 | 大对象数据正确存储和读取 | test_query_blob.py |
| **NULL 值** | NULL 值处理 | 插入和查询 NULL 值 | NULL 值正确处理 | test_stmt.py::test_stmt_null |

#### 7.1.4 **参数绑定测试（STMT）**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **STMT** | 单行绑定插入 | 使用 bind_param 绑定单行数据 | 数据插入成功 | test_stmt.py::test_stmt_insert |
| **STMT** | 多行批量绑定 | 使用 bind_param_batch 绑定多行数据 | 批量数据插入成功 | test_stmt.py::test_stmt_insert_multi |
| **STMT** | 设置表名和标签 | 使用 set_tbname_tags 设置表名和标签 | 自动创建子表并插入 | test_stmt.py::test_stmt_set_tbname_tag |
| **STMT** | NULL 值绑定 | 绑定 NULL 值到各列 | NULL 值正确插入 | test_stmt.py::test_stmt_null |
| **STMT** | 所有数据类型绑定 | 绑定所有支持的数据类型 | 各类型数据正确插入 | test_stmt.py::test_stmt_insert |
| **STMT** | WebSocket STMT | 通过 WebSocket 使用参数绑定 | 参数绑定正确执行 | taos-ws-py/tests/test_stmt.py |

#### 7.1.5 **参数绑定测试（STMT2）**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **STMT2** | prepare 预编译 | 使用 prepare 预编译 SQL | 预编译成功 | test_stmt2.py::test_stmt2_prepare_empty_sql |
| **STMT2** | 独立数组绑定 | 使用 bind_param 绑定独立数组 | 数据正确绑定和插入 | test_stmt2.py::insert_bind_param |
| **STMT2** | 独立表绑定 | 使用 bind_param_with_tables 绑定表数据 | 数据正确绑定和插入 | test_stmt2.py::insert_bind_param_with_tables |
| **STMT2** | 普通表插入 | STMT2 插入普通表数据 | 数据插入成功 | test_stmt2.py::insert_bind_param_normal_tables |
| **STMT2** | 查询绑定 | STMT2 执行参数绑定查询 | 查询结果正确返回 | test_stmt2.py::test_stmt2_query |
| **STMT2** | 无效参数校验 | 绑定无效参数类型 | 抛出相应异常 | test_stmt2.py::test_bind_invalid_tbnames_type |
| **STMT2** | 无效连接处理 | 使用已关闭连接创建 stmt2 | 返回 None 或抛出异常 | test_connection.py::test_stmt2_invalid_conn |
| **STMT2** | WebSocket STMT2 | 通过 WebSocket 使用 STMT2 | STMT2 正确执行 | test_stmt2.py::test_stmt2_example |

#### 7.1.6 **无模式写入测试（Schemaless）**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **InfluxDB 协议** | Line Protocol 写入 | 使用 InfluxDB 行协议写入数据 | 数据正确写入，自动建表 | test_lines.py::test_schemaless_insert |
| **OpenTSDB 协议** | Telnet Protocol 写入 | 使用 OpenTSDB telnet 协议写入 | 数据正确写入 | test_lines.py::test_schemaless_insert |
| **JSON 协议** | JSON Protocol 写入 | 使用 JSON 协议写入数据 | 数据正确写入 | test_lines.py::test_schemaless_insert |
| **TTL** | 带 TTL 写入 | 写入数据时指定 TTL | 数据写入成功，TTL 生效 | test_lines.py::test_schemaless_insert_ttl |
| **req_id** | 带 req_id 写入 | 写入时携带 req_id | 写入成功，便于追踪 | test_lines.py::test_schemaless_insert_with_req_id |
| **Raw 写入** | Raw 格式写入 | 使用 schemaless_insert_raw 写入 | 数据正确写入 | test_lines.py::test_schemaless_insert_raw |
| **数据更新** | Schemaless 更新 | 重复时间戳数据更新 | 数据正确更新 | test_lines.py::test_schemaless_insert_update_2 |
| **错误处理** | 无效格式处理 | 写入无效格式数据 | 抛出 SchemalessError 异常 | test_lines.py::test_schemaless_insert |

#### 7.1.7 **数据订阅测试（TMQ）**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **消费者** | 创建 Consumer | 使用配置创建 TMQ 消费者 | 消费者创建成功 | test_tmq.py::test_consumer_with_precision |
| **订阅** | 订阅主题 | 订阅指定的 topic | 订阅成功 | test_tmq.py::test_tmq_list_topics |
| **消费** | poll 消费数据 | 调用 poll 获取消息 | 正确获取写入的数据 | test_tmq.py::test_tmq_assignment |
| **提交** | commit 偏移量 | 手动提交消费偏移量 | 偏移量提交成功 | test_tmq.py::test_tmq_committed_and_position |
| **分配** | assignment 获取分配 | 获取消费者分区分配 | 返回正确的分配信息 | test_tmq.py::test_tmq_assignment |
| **定位** | seek 偏移量 | 设置分区偏移量位置 | 偏移量设置成功 | test_tmq.py::test_tmq_seek |
| **查询** | committed/position | 查询已提交和当前偏移量 | 返回正确的偏移量 | test_tmq.py::test_tmq_committed_and_position |
| **精度** | 不同时间精度 | 测试 ms/us/ns 精度订阅 | 各精度数据正确消费 | test_tmq.py::test_consumer_with_precision |
| **取消订阅** | unsubscribe | 取消主题订阅 | 订阅成功取消 | test_tmq.py |
| **WebSocket TMQ** | WS 数据订阅 | 通过 WebSocket 进行数据订阅 | 订阅和消费正常 | taos-ws-py/tests/test_tmq.py |

#### 7.1.8 **Cursor 接口测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **PEP249** | execute | cursor.execute 执行 SQL | SQL 正确执行 | test_native_cursor.py::test_cursor |
| **PEP249** | executemany | 批量执行 SQL | 批量 SQL 正确执行 | test_native_many.py |
| **PEP249** | fetchone | 获取单行结果 | 返回单行数据 | test_rest_connection.py::test_fetch_one |
| **PEP249** | fetchall | 获取所有结果 | 返回所有行数据 | test_rest_connection.py::test_fetch_all |
| **PEP249** | fetchmany | 获取指定行数结果 | 返回指定行数数据 | taos-ws-py/tests/test_cursor.py |
| **PEP249** | description | 获取列描述信息 | 返回列名、类型等信息 | test_rest_connection.py::test_fetch_all |
| **PEP249** | rowcount | 获取影响行数 | 返回正确的行数 | test_rest_connection.py::test_row_count |
| **PEP249** | close | 关闭游标 | 游标正常关闭 | test_native_cursor.py |

#### 7.1.9 **SQLAlchemy 集成测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **taos** | Native SQLAlchemy | 使用 taos:// 协议 | SQLAlchemy 正常工作 | test_sqlalchemy.py::test_read_from_sqlalchemy_taos |
| **taosws** | WebSocket SQLAlchemy | 使用 taosws:// 协议 | SQLAlchemy 正常工作 | test_sqlalchemy.py::test_read_from_sqlalchemy_taosws |
| **taosrest** | REST SQLAlchemy | 使用 taosrest:// 协议 | SQLAlchemy 正常工作 | test_sqlalchemy.py::test_read_from_sqlalchemy_taosrest |
| **元数据** | get_schema_names | 获取数据库列表 | 返回正确的数据库列表 | test_sqlalchemy.py::check_basic |
| **元数据** | get_table_names | 获取表列表 | 返回正确的表列表 | test_sqlalchemy.py::check_basic |
| **元数据** | get_columns | 获取列信息 | 返回正确的列信息 | test_sqlalchemy.py::check_basic |
| **参数绑定** | 格式化语句 | SQLAlchemy 参数绑定查询 | 查询正确执行 | test_sqlalchemy.py::test_stmt2_query |

### 7.2 **性能测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **查询性能** | 单线程拉取 meters 表数据，Native 连接 | 性能不低于 100W 行/秒 | perf_test_query.py |
| **SQL 写入性能** | 单线程 SQL 写入 meters 表，Native 连接 | 性能不低于 10W 行/秒 | perf_test_insert.py |
| **STMT 写入性能** | 单线程 STMT 参数绑定写入 meters 表 | 性能不低于 100W 行/秒 | perf_test_stmt.py |
| **STMT2 写入性能** | 单线程 STMT2 参数绑定写入 meters 表 | 性能不低于 100W 行/秒 | perf_test_stmt2.py |
| **订阅性能** | 单线程 TMQ 消费数据，Native 连接 | 性能不低于 10W 行/秒 | perf_test_tmq.py |
| **WebSocket 查询** | 单线程 WebSocket 查询性能 | 性能达到 Native 80% 以上 | perf_test_ws_query.py |
| **WebSocket 写入** | 单线程 WebSocket STMT2 写入性能 | 性能达到 Native 80% 以上 | perf_test_ws_stmt2.py |

### 7.3 **安全性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **凭证安全** | 从环境变量读取凭证 | 正确读取凭证，连接成功 | test_connect_args.py |
| **凭证安全** | Token 认证连接云服务 | Token 正确传递，认证成功 | test_rest_connection.py::test_token |
| **凭证安全** | 错误 Token 处理 | 返回 401 认证错误 | test_rest_connection.py::test_wrong_token |
| **传输加密** | WSS 加密连接 | SSL/TLS 加密生效 | test_sqlalchemy.py::test_read_from_sqlalchemy_taosws |
| **SQL 注入防护** | 参数绑定防注入 | 参数正确转义，无注入风险 | test_stmt.py, test_stmt2.py |
| **特殊字符** | 密码含特殊字符 | 特殊字符正确处理 | test_rest_connection.py::test_special_characters |
| **日志脱敏** | 日志输出脱敏 | 密码/Token 不在日志中明文出现 | test_cursor_logfile.py |
| **错误处理** | 生产环境错误信息 | 错误信息简化，不暴露内部细节 | test_exception.py |

### 7.4 **稳定性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **长时间运行** | 连续运行 24 小时写入和查询 | 无内存泄漏，功能正常 | stability_test_24h.py |
| **连接池** | 使用 dbutils 连接池 | 连接正确复用和回收 | test_dbutils_conntect_pool.py |
| **SQLAlchemy 连接池** | SQLAlchemy 连接池管理 | 连接池正常工作 | test_sqlalchemy_conntect_pool.py |
| **并发写入** | 多线程并发写入 | 数据正确写入，无竞态条件 | stability_test_concurrent.py |
| **并发订阅** | 多消费者并发消费 | 数据正确分发，无丢失 | test_tmq.py |
| **异常恢复** | 网络断开后重连 | 自动重连或正确抛出异常 | stability_test_reconnect.py |
| **资源释放** | 反复创建关闭连接 | 资源正确释放，无泄漏 | stability_test_resource.py |
| **查询超时** | 慢查询超时处理 | 超时后正确终止查询 | test_query.py |

### 7.5 **兼容性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **TDengine 3.3.2.0** | 与 TDengine 3.3.2.0 兼容性 | 所有功能正常 | CI workflow |
| **TDengine 3.4.0.0** | 与 TDengine 3.4.0.0 兼容性 | 所有功能正常，新特性可用 | CI workflow |
| **Python 3.7** | Python 3.7 环境运行 | 所有功能正常 | CI workflow |
| **Python 3.8** | Python 3.8 环境运行 | 所有功能正常 | CI workflow |
| **Python 3.9** | Python 3.9 环境运行 | 所有功能正常 | CI workflow |
| **Python 3.10** | Python 3.10 环境运行 | 所有功能正常 | CI workflow |
| **Python 3.11** | Python 3.11 环境运行 | 所有功能正常 | CI workflow |
| **Python 3.12** | Python 3.12 环境运行 | 所有功能正常 | CI workflow |
| **Ubuntu** | Ubuntu 22.04 环境 | 所有功能正常 | CI workflow |
| **CentOS** | CentOS 7/8 环境 | 所有功能正常 | CI workflow |
| **macOS** | macOS 环境 | 所有功能正常 | CI workflow |
| **Windows** | Windows 10/11 环境 | 所有功能正常 | CI workflow |
| **Native + WebSocket** | 同时使用两种连接方式 | 两种方式互不干扰 | test_query.py (IS_WS) |

## 8. **测试计划**

1. 测试环境搭建：0.5 人天
  - TDengine 服务器部署和配置
  - taosAdapter 服务配置
  - 测试客户端环境准备（多 Python 版本、多操作系统）
1. 测试执行：5 人天
  - 功能测试：2 人天
  - 性能测试：1 人天
  - 安全性测试：0.5 人天
  - 稳定性测试：1 人天
  - 兼容性测试：0.5 人天
1. 测试总结：0.5 人天
  - 测试报告编写
  - 问题整理和跟踪

## 9. **风险评估**

| **风险项** | **风险描述** | **影响程度** | **缓解措施** |
| --- | --- | --- | --- |
| **环境依赖** | 原生连接依赖 taosc 库，不同平台安装方式不同 | 中 | 提供详细的安装文档，CI 覆盖多平台 |
| **版本兼容** | TDengine 新版本可能引入不兼容变更 | 中 | 持续集成测试，及时发现兼容性问题 |
| **性能波动** | 不同测试环境性能存在差异 | 低 | 使用固定配置的测试环境，多次测量取平均值 |
| **网络不稳定** | WebSocket 连接可能受网络影响 | 中 | 实现重连机制，测试网络异常场景 |
| **安全漏洞** | 第三方依赖可能存在安全漏洞 | 中 | CI 集成依赖安全扫描，定期更新依赖 |

## 10. **参考文档**

1. Python 连接器-Requirement Spec
2. Python 连接器-Function Spec
3. Python 连接器-Design Spec
4. [TDengine 官方文档](https://docs.taosdata.com/)
5. [PEP 249 - Python Database API Specification v2.0](https://www.python.org/dev/peps/pep-0249/)
6. [SQLAlchemy 官方文档](https://www.sqlalchemy.org/)

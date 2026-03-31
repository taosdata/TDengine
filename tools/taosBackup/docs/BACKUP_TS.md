# 功能测试报告（Test Spec）- taosBackup

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-31 | 2026-03-31 | 1.0 | Alex Duan | 初版创建，基于测试用例反推 |

# 测试目标

- 验证 taosBackup 备份/恢复功能的正确性，包括全量备份、增量备份、断点续传
- 验证所有 TDengine 数据类型的备份恢复精度
- 验证命令行参数解析和异常输入处理
- 验证 Binary（.dat）和 Parquet（.par）两种存储格式的正确性
- 验证 Schema 变更场景下的恢复兼容性
- 验证虚拟表、主键表、中文标识符等特殊表类型
- 验证多线程并发、连接池重试、信号处理等稳定性
- 验证进度显示在 TTY 和非 TTY 模式下的正确性

# 参考文档

- BACKUP_RS.md（需求规格说明书）
- BACKUP_FS.md（概要设计说明书）
- BACKUP_DS.md（详细设计说明书）

# 测试结论

测试覆盖 18 个测试文件，共 68 个测试用例，覆盖功能、异常、边界、性能和兼容性场景。

# 测试环境

- OS: Linux（x86_64）
- TDengine: v3.3+（支持 STMT2）
- Python: 3.x + pytest
- 测试框架: tdLog / tdSql / etool 自定义工具库

# 功能测试

## 基础功能

### 测试要点

验证 taosBackup 的基本备份恢复流程：taosBenchmark 造数据 → 备份 → 恢复到新数据库 → 数据比对。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 1 | test_taosbackup_commandline | 完整命令行备份恢复流程：taosBenchmark 造全类型数据 → taosBackup 备份 → 恢复 → 数据比对 | test_taosbackup_commandline.py |
| 2 | test_taosbackup_all_databases | 不指定 -D 参数时备份所有数据库（触发 getAllDatabases() 路径） | test_taosbackup_commandline.py |

## 数据类型测试

### 测试要点

验证所有 TDengine 支持的数据类型在备份恢复后的精度和正确性，包括边界值和 NULL 值。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 3 | test_taosbackup_datatypes | 验证 BIGINT/INT/SMALLINT/TINYINT/BOOL/FLOAT/DOUBLE/BINARY/NCHAR/TIMESTAMP/VARBINARY/GEOMETRY 及各自 UNSIGNED 变体的边界值和 NULL 值备份恢复 | test_taosbackup_datatypes.py |

## 断点续传测试

### 测试要点

验证恢复过程中断后可从 checkpoint 断点续传，跳过已恢复文件。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 4 | test_taosbackup_checkpoint | 100 子表 × 200,000 行 → 备份 → 恢复中途中断 → 再次恢复从断点继续（验证 restore_checkpoint.txt 和 256 桶哈希表） | test_taosbackup_checkpoint.py |
| 5 | test_checkpoint_large_subtable | 大子表场景验证 restoreCkpt.c 哈希表 init/insert/lookup/free 路径 | test_taosbackup_checkpoint.py |

## 中文和特殊字符测试

### 测试要点

验证中文和特殊字符标识符在备份恢复中的正确性。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 6 | test_taosbackup_chinese | 中文数据库/超级表/子表/普通表名、中文列名和标签名的备份恢复 | test_taosbackup_chinese.py |

## 时间精度测试

### 测试要点

验证毫秒/微秒/纳秒精度数据库的备份恢复精度。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 7 | test_taosbackup_precision | 纳秒精度数据库 10 子表 × 100 行的备份恢复精度验证 | test_taosbackup_precision.py |

## 主键表测试

### 测试要点

验证包含主键的表在备份恢复后的正确性。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 8 | test_taosbackup_primarykey | 使用 primaryKey.json 构建主键表 → 备份 → 恢复 → 数据比对 | test_taosbackup_primarykey.py |

## Schema 变更测试

### 测试要点

验证源端和目标端 Schema 不一致时的恢复兼容性（列增删、类型变更等）。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 9 | test_taosbackup_schema_change | 先用 schemaChange.json 造数据并备份，再用 schemaChangeNew.json 变更 Schema → 恢复 → 验证部分列映射和缺失列处理 | test_taosbackup_schema_change.py |

## 虚拟表测试

### 测试要点

验证三种虚拟表类型（VIRTUAL_NORMAL_TABLE / VIRTUAL_CHILD_TABLE / VIRTUAL_SUPER_TABLE）的备份恢复。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 10 | test_taosbackup_vtable | 全量备份恢复虚拟表：验证 vtb.sql 执行和虚拟标签恢复 | test_taosbackup_vtable.py |

## 存储格式测试

### 测试要点

验证 Binary 和 Parquet 两种存储格式在 STMT1/STMT2 下的正确性。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 11 | test_taosbackup_format | Binary × STMT1、Binary × STMT2、Parquet × STMT1、Parquet × STMT2 四种组合的备份恢复验证 | test_taosbackup_format.py |
| 12 | test_parquet_null_and_timestamp_tags | Parquet 格式下 NULL 值和 TIMESTAMP 标签的恢复验证 | test_taosbackup_format.py |

## 全 NULL 列测试

### 测试要点

验证所有列值均为 NULL 时的 COL_LEN_ALL_NULL 优化路径。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 13 | test_taosbackup_allnull | 所有支持类型的全 NULL 列跳过优化验证 | test_taosbackup_allnull.py |

## 数据完整性测试

### 测试要点

验证多数据库、线程数变化、数据库属性保留、增量备份、空库等复合场景。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 14 | test_taosbackup_integrity | 多 DB 备份、不同线程数、DB 属性保留、增量备份、空数据库等综合完整性验证 | test_taosbackup_integrity.py |

## 进度显示测试

### 测试要点

验证 bckProgress.c 在 TTY 和非 TTY（管道）模式下的输出行为。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 15 | test_progress_tty_mode | TTY 模式下 1 秒刷新、\r\033[K 覆盖行输出验证 | test_taosbackup_progress.py |
| 16 | test_progress_pipe_30s_tick | 非 TTY（管道）模式下 30 秒间隔输出验证 | test_taosbackup_progress.py |
| 17 | test_restore_progress_tty_mode | 恢复模式下 TTY 进度显示验证（isRestore=1） | test_taosbackup_progress.py |
| 18 | test_restore_progress_pipe_30s_tick | 恢复模式下管道进度显示验证（isRestore=1） | test_taosbackup_progress.py |

# 异常和容错测试

### 测试要点

验证异常场景下 taosBackup 的容错能力：服务不可用、文件损坏、权限不足、信号中断等。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 19 | test_backup_server_pause_before_connect | 备份前停止 taosd → 验证连接池指数退避重试 → 重新启动 taosd 后连接成功 | test_taosbackup_errors.py |
| 20 | test_backup_all_databases | 不指定 -D 时触发 SHOW DATABASES 路径验证 | test_taosbackup_errors.py |
| 21 | test_restore_corrupted_parquet | 覆写 .par 文件前 128 字节后恢复 → 验证错误处理 | test_taosbackup_errors.py |
| 22 | test_restore_large_binary | BINARY(16000) 列 ~15000 字节数据 → 验证 realloc 大缓冲区路径 | test_taosbackup_errors.py |
| 23 | test_sigint_during_backup | 备份过程中发送 SIGINT → 验证优雅退出 | test_taosbackup_errors.py |
| 24 | test_sigint_during_restore | 恢复过程中发送 SIGINT → 验证优雅退出 | test_taosbackup_errors.py |
| 25 | test_backup_path_create_failure | 输出路径父目录为普通文件 → 验证 mkdir 失败错误处理 | test_taosbackup_errors.py |
| 26 | test_backup_stbsql_dir_collision | 预创建 stb.sql 为目录 → 验证 taosOpenFile 失败处理 | test_taosbackup_errors.py |
| 27 | test_taosbackup_retry | 备份恢复中的异常重试逻辑验证 | test_taosbackup_except.py |
| 28 | test_taosbackup_server_restart_backup | 备份过程中 taosd 停止 → 验证连接重试和恢复 | test_taosbackup_except.py |
| 29 | test_taosbackup_server_restart_restore | 恢复过程中 taosd 停止 → 验证连接重试和恢复 | test_taosbackup_except.py |
| 30 | test_taosbackup_restore_retry | 恢复过程中 taosadapter kill/restart → 验证 WebSocket 重试 | test_taosbackup_except.py |
| 31 | test_taosbackup_thread_creation_failure | pthread_create 中途失败 → 验证进程能干净退出 | test_taosbackup_except.py |

# 边界测试

### 测试要点

验证边界条件：长表名、长路径、调试模式、认证参数、热备份等。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 32 | test_taosbackup_edge | 长名称（接近 64 字符限制）、长输出路径、-g 调试模式、用户名密码、热备份（备份期间写入）等边界验证 | test_taosbackup_edge.py |

# Bug 回归测试

### 测试要点

验证已修复 Bug 不再复现。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 33 | test_taosbackup_bugs | 验证 TS-7053：-S/-E 起止时间在所有时间精度（ms/us/ns）下的正确性 | test_taosbackup_bugs.py |

# 覆盖率补充测试

### 测试要点

补充代码覆盖率，覆盖各个内部分支和路径。

### 用例列表

| # | 测试用例 | 测试描述 | 测试文件 |
| --- | --- | --- | --- |
| 34 | test_parquet_backup_restore_stmt2 | Parquet 格式 + STMT2 恢复路径 | test_taosbackup_coverage.py |
| 35 | test_parquet_with_all_null_columns | Parquet 格式全 NULL 列备份恢复 | test_taosbackup_coverage.py |
| 36 | test_time_filter_backup | --start/--end 时间过滤备份 | test_taosbackup_coverage.py |
| 37 | test_invalid_arg_combos | 无效参数组合校验（验证 taosBackup 拒绝非法输入） | test_taosbackup_coverage.py |
| 38 | test_sigint_graceful_exit | SIGINT 信号优雅退出（覆盖率补充） | test_taosbackup_coverage.py |
| 39 | test_nchar_multibyte_native_format | 多字节 NCHAR 列 native 格式备份恢复 | test_taosbackup_coverage.py |
| 40 | test_large_binary_column | 接近最大长度 BINARY 列备份恢复 | test_taosbackup_coverage.py |
| 41 | test_specific_table_backup | 位置参数指定子表备份 | test_taosbackup_coverage.py |
| 42 | test_parquet_stmt1_restore | Parquet 格式 + STMT1 恢复路径 | test_taosbackup_coverage.py |
| 43 | test_backup_with_native_dsn | --dsn 参数 native 连接备份 | test_taosbackup_coverage.py |
| 44 | test_connection_pool_retry_on_pause | 暂停 taosd 触发连接池重试 | test_taosbackup_coverage.py |
| 45 | test_geometry_tag_backup_restore | GEOMETRY 列标签备份恢复 | test_taosbackup_coverage.py |
| 46 | test_normal_table_backup_restore | 仅普通表（无超级表）数据库备份恢复 | test_taosbackup_coverage.py |
| 47 | test_mixed_stb_and_ntb | 超级表 + 普通表混合数据库备份恢复 | test_taosbackup_coverage.py |
| 48 | test_null_tag_backup_restore | NULL 标签值子表恢复（restoreMeta.c isNull 分支） | test_taosbackup_coverage.py |
| 49 | test_timestamp_tag_backup_restore | TIMESTAMP 标签恢复（restoreMeta.c TIMESTAMP case 分支） | test_taosbackup_coverage.py |
| 50 | test_empty_child_table_skip | 空子表跳过（backupData.c 空表分支） | test_taosbackup_coverage.py |
| 51 | test_checkpoint_resume_with_existing_entries | 断点续传哈希表功能（init/insert/lookup/free 路径） | test_taosbackup_coverage.py |
| 52 | test_readonly_output_dir_checkpoint | 只读输出目录时 g_allowWriteCP=false 路径 | test_taosbackup_coverage.py |
| 53 | test_backup_with_extra_args | -k/-z/-m 额外参数（retry-count / retry-sleep-ms / tag-thread-num） | test_taosbackup_coverage.py |
| 54 | test_restore_stmt_version1 | STMT v1 恢复路径（restoreStmt.c） | test_taosbackup_coverage.py |
| 55 | test_database_with_only_ntb | 仅普通表数据库恢复（restoreMeta.c 无超级表提前返回） | test_taosbackup_coverage.py |
| 56 | test_large_tag_array_capacity | 标签文件数组扩容（restoreMeta.c tagFiles[] realloc 倍增） | test_taosbackup_coverage.py |
| 57 | test_schema_change_partial_column_write | Schema 变更后部分列 INSERT（bckSchemaChange.c 部分列映射） | test_taosbackup_coverage.py |
| 58 | test_checkpoint_file_loading_path | 断点文件加载路径（restoreCkpt.c 文件加载分支） | test_taosbackup_coverage.py |
| 59 | test_multi_db_backup_restore | 逗号分隔多数据库备份（-D db1,db2） | test_taosbackup_coverage.py |
| 60 | test_multi_rename_restore | 多对重命名恢复（-W db1=n1\|db2=n2） | test_taosbackup_coverage.py |
| 61 | test_second_restore_already_exists | 二次恢复触发 table/stable already-exists 分支 | test_taosbackup_coverage.py |
| 62 | test_invalid_dat_magic | .dat 文件 magic 错误时恢复优雅失败 | test_taosbackup_coverage.py |
| 63 | test_backup_resume_mode | -C 恢复模式跳过已完成子表 | test_taosbackup_coverage.py |

# 性能测试

性能测试通过独立 benchmark 脚本执行，非 pytest 用例。

参考 benchmark.md 实测数据：

- 备份速度：约 1.87M rows/s（1000 万行 → 5.35 秒）
- 恢复速度：STMT2 约 0.76M rows/s（1000 万行 → 13.12 秒）
- 压缩比：Binary 格式约 7.85:1

# 安全测试

| # | 测试项 | 测试描述 | 覆盖用例 |
| --- | --- | --- | --- |
| 1 | 认证参数 | -u/-p 用户名密码传递 | test_taosbackup_edge |
| 2 | DSN 凭证 | --dsn 包含认证信息 | test_backup_with_native_dsn |
| 3 | 权限不足 | 只读输出目录场景 | test_readonly_output_dir_checkpoint |

# 兼容性测试

| # | 测试项 | 测试描述 | 覆盖用例 |
| --- | --- | --- | --- |
| 1 | STMT1/STMT2 兼容 | -v 1 / -v 2 两种 STMT 版本 | test_taosbackup_format, test_restore_stmt_version1 |
| 2 | Binary/Parquet 互操作 | -F binary / -F parquet 两种格式 | test_taosbackup_format, test_parquet_backup_restore_stmt2 |
| 3 | Schema 变更兼容 | 源端和目标端 Schema 不一致时的列映射 | test_taosbackup_schema_change, test_schema_change_partial_column_write |
| 4 | 时间精度兼容 | ms/us/ns 三种精度数据库 | test_taosbackup_precision, test_taosbackup_bugs |
| 5 | 二次恢复幂等 | 重复恢复到已有库 | test_second_restore_already_exists |

# 已知问题和限制

- Parquet 格式在 Windows 上不可用（编译时通过 `#ifndef TD_WINDOWS` 禁用）
- STMT2 多表模式仅在 Native 连接下可用，WebSocket 连接自动回退到单表模式
- DECIMAL 类型通过文本中转（decimalToStr），可能存在精度边界影响
- 测试框架依赖 taosBenchmark 造数据，需安装 TDengine Tools

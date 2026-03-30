# 存储安全 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-18 | 2025-12-30 | 1.0 | 鲍之骁 | 初稿 |

## 2. 测试目标

本测试规范用于验证 TDengine 存储安全特性（JIRA TS-7230）的功能完整性、安全性、性能和兼容性。
主要测试目标包括：
- 密钥生成与管理功能（taosk 工具）
- 密钥分发与备份恢复机制
- 透明加密功能（配置文件、元数据、数据文件）
- 加密状态查看与监控
- 多种加密算法支持（SM4、AES）
- 性能影响评估
- 安全性验证
- 版本兼容性测试

## 3. 参考文档

[存储安全 FS](https://taosdata.feishu.cn/wiki/KojBwzktkihgLRk2YIocWwFInxb)

## 4. 测试结论

功能符合预期

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 密钥生成（taosk 工具）

#### 6.1.1 测试要点

测试 taosk 工具的密钥生成功能，包括自动生成密钥、指定密钥、选择加密算法等。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 执行 taosk --help | 检查帮助信息是否完整，包含所有命令行参数说明 | 通过 |
| 2 | 执行 taosk --version | 检查版本信息输出正确 | 通过 |
| 3 | 生成默认密钥（SM4算法） | `taosk -c /etc/taos --encrypt-server --encrypt-database` 检查是否成功生成 master.bin 和 derived.bin 文件 | 通过 |
| 4 | 指定 SVR_KEY 和 DB_KEY | `taosk -c /etc/taos --encrypt-server mysvr123 --encrypt-database mydb456` 检查密钥文件生成，并验证可以正常解密 | 通过 |
| 5 | 启用配置文件加密 | `taosk -c /etc/taos --encrypt-server --encrypt-database --encrypt-config` 检查 derived.bin 包含 CFG_KEY | 通过 |
| 6 | 启用元数据加密 | `taosk -c /etc/taos --encrypt-server --encrypt-database --encrypt-metadata` 检查 derived.bin 包含 META_KEY | 通过 |
| 7 | 启用数据文件加密 | `taosk -c /etc/taos --encrypt-server --encrypt-database --encrypt-data` 检查 derived.bin 包含 DATA_KEY | 通过 |
| 8 | 指定 DATA_KEY（兼容历史版本） | `taosk -c /etc/taos --encrypt-server --encrypt-database --encrypt-data oldkey123` 检查 DATA_KEY 为指定值 | 通过 |
| 11 | 同时启用所有加密选项 | `taosk -c /etc/taos --encrypt-server --encrypt-database --encrypt-config --encrypt-metadata --encrypt-data` 检查所有密钥正确生成 | 通过 |
| 13 | 检查机器码绑定 | 在不同机器上尝试使用同一密钥文件，应该解密失败 | 通过 |
| 14 | 密钥长度验证 | 测试过短（<8字符）和过长（>16字符）的密钥，应该报错 | 通过 |

### 6.2 密钥更新

#### 6.2.1 测试要点

测试密钥更新功能，包括更新 SVR_KEY 和 DB_KEY，验证版本号递增。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 通过 taosk 更新 SVR_KEY | `taosk -c /etc/taos --update-svrkey "newsvr123"` 检查密钥更新成功，版本号递增 | 通过 |
| 2 | 通过 taosk 更新 DB_KEY | `taosk -c /etc/taos --update-dbkey "newdb456"` 检查密钥更新成功，版本号递增 | 通过 |
| 3 | 通过 taosk 同时更新 SVR_KEY 和 DB_KEY | `taosk -c /etc/taos --update-svrkey newsvr --update-dbkey newdb` 检查两个密钥都更新成功 | 通过 |
| 4 | 更新后 derived keys 自动重新生成 | 更新 DB_KEY 后，检查 derived.bin 文件被重新生成，时间戳更新 | 通过 |
| 5 | 更新时间戳验证 | 检查 svrKeyUpdateTime 或 dbKeyUpdateTime 更新为当前时间 | 通过 |
| 6 | 通过 SQL 命令更新 SVR_KEY | `alter system set svr_key 'newsvr123';` 检查密钥更新成功，版本号递增 | 通过 |
| 7 | 通过 SQL 命令更新 DB_KEY | `alter system set db_key 'newdb456';` 检查密钥更新成功，版本号递增 | 通过 |

### 6.3 密钥备份与恢复

#### 6.3.1 测试要点

测试跨机器的密钥备份和恢复功能，验证机器码绑定。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 备份密钥（需要验证 SVR_KEY） | `taosk -c /etc/taos --backup --svr-key "mysvr123"` 检查生成 master.bin.backup.xxx 文件 | 通过 |
| 2 | 备份时 SVR_KEY 验证失败 | 使用错误的 SVR_KEY 执行备份，应该报错提示验证失败 | 通过 |
| 3 | 未提供 SVR_KEY 执行备份 | `taosk -c /etc/taos --backup` 应该提示需要提供 --svr-key 参数 | 通过 |
| 4 | 恢复密钥到新机器 | 在机器A备份，复制备份文件到机器B `taosk -c /etc/taos --restore --machine-code /path/to/backup --svr-key mysvr123` 检查在机器B生成新的 master.bin，绑定到机器B的机器码 | 通过 |
| 5 | 恢复时 SVR_KEY 错误 | 使用错误的 SVR_KEY 执行恢复，应该解密失败 | 通过 |
| 6 | 未提供备份文件路径 | `taosk --restore --svr-key mysvr123` 应该提示需要提供 --machine-code 参数 | 通过 |

### 6.4 透明加密

#### 6.4.1 测试要点

测试系统的透明加密功能，验证加密状态标识。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 集群启用配置文件加密 | 生成包含 CFG_KEY 的密钥，启动 taosd，检查 dnode.json、mnode.json、vnode.json 等文件被加密，检查加密后的配置文件开头包含 "tdEncrypt" 魔法数字和算法标识，加密后的文件不可读 | 通过 |
| 2 | 集群启用元数据加密 | 检查 sdb 和 snode 是否被正确加密 | 通过 |
| 3 | 集群启用数据文件加密 | `CREATE DATABASE db1 ENCRYPT_ALGORITHM 'sm4';` 检查数据库创建成功，确认 tsdb wal bse 等目录下的数据文件正确被加密 | 通过 |

### 6.5 密钥到期策略

#### 6.5.1 测试要点

测试密钥到期时间和策略的设置与执行。

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 设置密钥到期时间（ALARM 策略) | `ALTER SYSTEM SET KEY_EXPIRATION 90 DAYS STRATEGY 'ALARM';` 检查设置成功,密钥过期后，在日志中提醒用户密钥过期 | 通过 |

### 6.6 加密状态查看

#### 6.6.1 测试要点

测试通过系统表查看加密状态的功能。

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 查看系统加密状态 | `SELECT * FROM information_schema.ins_encrypt_status;` 检查返回配置文件、元数据、数据文件的加密状态和算法 | 通过 |
| 2 | 查看数据库加密算法 | `SELECT name, encrypt_algorithm FROM information_schema.ins_databases;` 检查返回每个数据库的加密算法 | 通过 |

### 6.7 配置文件行为变更

#### 6.7.1 测试要点

测试启用存储安全后 taos.cfg 配置文件的行为变更。

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 配置文件加密 | 生成包含 CFG_KEY 的密钥，启动 taosd 。 配置文件被正确加密，无法查看修改。 | 通过 |
| 2 | 配置文件防篡改 | 启动 taosd , 修改配置文件, 重启 taosd 。 修改的配置参数没有被加载到系统中。 | 通过 |

## 7. 易用性测试（可选）

## 8. 长期稳定性测试（可选）

## 9. 性能测试

等同于 [加密算法 TS](https://taosdata.feishu.cn/wiki/SWA4wKo42idV9SkvhCfcYVyJn2o)。

## 10. 安全测试

#### 10.0.1 测试要点

检查加密后是否能在数据文件中看到明文

#### 10.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WAL 文件明文检查 | 创建加密数据库，写入包含特定字符串（如"SecretData123"）的数据 使用 strings、grep 等工具检查 WAL 文件，验证不包含明文字符串 | 通过 |
| 2 | TSDB 文件明文检查 | 写入数据后，使用工具检查 TSDB 文件，验证不包含明文字符串 | 通过 |
| 3 | STT 文件明文检查 | 触发 STT 文件生成，检查文件不包含明文数据 | 通过 |
| 4 | 配置文件明文检查 | 检查加密的 dnode.json、mnode.json 等配置文件不包含明文配置信息 | 通过 |
| 5 | 元数据文件明文检查 | 创建用户和密码，检查 SDB 文件不包含明文 | 通过 |
| 6 | 密钥文件明文检查 | 检查 master.bin 和 derived.bin 不包含明文 | 通过 |

## 11. 兼容性测试

#### 11.0.1 测试要点

检查加密后是否能在数据文件中看到明文

#### 11.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 未加密集群升级 | 从不支持存储安全的版本升级到新版本，验证能正常启动和运行 | 通过 |
| 2 | 历史 SM4 加密升级 | 从仅支持 SM4 的历史版本升级，使用 taosk 指定历史 DATA_KEY，验证能正常读写数据 | 通过 |
| 3 | 自动加密配置文件 | 生成包含 CFG_KEY 的密钥，升级 taosd。 检查 dnode.json、mnode.json、vnode.json 等文件被加密。 | 通过 |
| 4 | 自动加密元数据文件 | 生成包含 META_KEY 的密钥，升级 taosd。 检查 sdb,checkpoint 等文件被加密。 | 通过 |

## 12. 已知问题和限制（可选）

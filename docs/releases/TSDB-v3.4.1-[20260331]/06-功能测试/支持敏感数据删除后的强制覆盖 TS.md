# 支持敏感数据删除后的强制覆盖 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-3-13 | 2026-3-13 | 0.1 | 鲍之骁 | 初稿 |

## 2. 测试目标

<quote-container>
测试敏感数据删除后的强制覆盖功能
</quote-container>

## 3. 参考文档

<quote-container>
[支持敏感数据删除后的强制覆盖 - FS](https://taosdata.feishu.cn/wiki/EM2cwmPefiqDiyklah4cKzgLnTd)
</quote-container>

## 4. 测试结论

<quote-container>
功能符合预期
</quote-container>

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 测试数据库选项 SECURE_DELETE

#### 6.1.1 测试要点

测试数据库选项 SECURE_DELETE 开关

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 创建数据库并设置 SECURE_DELETE 为 1 | 删除数据后，查看 tsdb 文件，数据已经不存在 | 通过 |

### 6.2 测试超级表选项 SECURE_DELETE

#### 6.2.1 测试要点

测试超级表选项 SECURE_DELETE 开关，以及优先级

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 创建超级表并设置 SECURE_DELETE 为 1 | 删除数据后，查看 tsdb 文件，数据已经不存在 | 通过 |
| 2 | 创建数据库并设置 SECURE_DELETE 为 0 创建超级表并设置 SECURE_DELETE 为 1 | 超级表的 SECURE_DELETE 优先级高于数据库，删除数据后，查看 tsdb 文件，数据已经不存在 | 通过 |

### 6.3 测试数据删除选项 SECURE_DELETE

#### 6.3.1 测试要点

测试数据删除选项 SECURE_DELETE 开关

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 删除数据时指定 SECURE_DELETE | 删除数据后，查看 tsdb 文件，数据已经不存在 | 通过 |

## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无。

## 9. 性能测试

无。

## 10. 安全测试

#### 10.0.1 测试要点

检查数据删除后，是否能看到敏感数据。

#### 10.0.2 用例列表

```sql
create database db vgroups 1;      
use db;
create table stb(ts timestamp, v1 varchar(10)) tags (id int);                         
create table ctb using stb tags(1);                                                   
insert into ctb values(now(),'TDengine');                                               
insert into ctb values(now()+1s,'TDengine');                                               
insert into ctb values(now()+2s,'TDengine');                                               
insert into ctb values(now()+3s,'TDengine');                                               
insert into ctb values(now()+4s,'TDengine');     
flush database db;      
-- 正常删除，查看 stt 文件，发现其中包含 TDengine                                         
delete from ctb;

insert into ctb values(now(),'TDengine');                                               
insert into ctb values(now()+1s,'TDengine');                                               
insert into ctb values(now()+2s,'TDengine');                                               
insert into ctb values(now()+3s,'TDengine');                                               
insert into ctb values(now()+4s,'TDengine');     
flush database db;                        

--安全删除 ，查看 stt 文件，发现 TDengine 已经不存在                      
delete from ctb secure_delete;   
```

## 11. 兼容性测试

无

## 12. 已知问题和限制（可选）

无

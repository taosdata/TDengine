---
toc_max_heading_level: 4
title: "数据库加密"
sidebar_label: "数据库加密"
---

## 概述

本节简要介绍数据库加密功能，该功能从 3.3.0.0 版本的 TDengine 企业版中开始提供。系统管理员可以通过在创建数据库时指定加密算法来创建加密的数据库。

## 配置密钥

在创建加密数据库之前要先配置好加密密钥，有两种配置方式：离线方式和在线方式。

### 离线设置

可以使用下面的命令为每个节点配置密钥。

```shell
taosd -y {encryptKey}
```
密钥要大于等于8个字符，小于16个字符。密钥可包含大小写字母，数字，所有可打印的特殊字符。

### 在线设置

如果没有采用离线方式配置，并集群所有节点都在线时，可以使用如下 SQL 命令进行在线配置：

```sql
create encrypt_key 'value'
```
密钥要大于等于8个字符，小于16个字符。密钥可包含大小写字母，数字，所有可打印的特殊字符。

## 创建加密数据库

在创建数据库时可以通过指定 ENCRYPT_ALGORITHM 参数创建加密数据库。

```shell
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
   ENCRYPT_ALGORITHM {'none' | 'sm4'}
}
```
`sm4` 表示使用 sm4 算法，目前只支持这种加密算法，`none` 表示不加密。

## 修改加密算法

对已存在的库，目前不支持修改 ENCRYPT_ALGORITHM 参数，包括将未加密数据库改为加密，或者将加密数据库改为未加密。

## 查看数据库加密配置

通过以下的SQL命令可以查看数据库的加密配置：

```sql
select name, `encrypt_algorithm` from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

## 查看节点密钥状态

通过以下的SQL命令参看节点密钥状态：

```sql
show encryptions;

select * from information_schema.ins_encryptions;
  dnode_id   |           key_status           |
===============================================
           1 | loaded                         |
           2 | unset                          |
           3 | unknown                        |
```
key_status 有三种取值：
- 当节点未设置密钥时，状态列显示 unset。
- 当密钥被检验成功并且加载后，状态列显示 loaded.
- 当节点未启动，key的状态无法被探知时，状态列显示 unknown

## 更新密钥配置

当节点的硬件配置发生变更时，需要通过以下命令更新密钥，与离线配置密钥的命令相同：

```shell
taosd -y  {encryptKey}
```
更新密钥配置，需要先停止 taosd，并且使用完全相同的密钥，也即密钥在数据库创建后不能修改。

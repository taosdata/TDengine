---
toc_max_heading_level: 4
title: "数据库加密"
sidebar_label: "数据库加密"
---

## 概述
采用用户指定的加密算法加密数据库中的数据。

## 配置密钥

### 离线设置
为每个节点配置密钥。通过以下命令完成：
```
taosd -y {encryptKey}
```
密钥要大于8个字符，小于16个字符。密钥可包含大小写字母，数字，所有特殊字符。

### 在线设置
当集群所有节点都在线时，可以使用如下taos shell SQL命令：
```
create encrypt_key 'value'
```
密钥要大于8个字符，小于16个字符。密钥可包含大小写字母，数字，所有特殊字符。

## 创建加密数据库
通过指定 ENCRYPT_ALGORITHM 参数创建加密数据库。
```
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
   ENCRYPT_ALGORITHM {'none' | 'sm4'}
}
```
sm4表示使用 sm4 算法。

## 修改数据库
对已存在的库，不支持修改 ENCRYPT_ALGORITHM 参数，包括将未加密数据库改为加密，或者将加密数据库改为未加密。

## 查看数据库加密配置
通过以下的SQL命令参看数据库的加密配置：
```
select name, `encrypt_algorithm` from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

## 查看节点密钥状态
通过以下的SQL命令参看节点密钥状态：
```
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
```
taosd -y  {encryptKey}
```
更新密钥配置，需要先停止taosd，并且使用完全相同的密钥，也即密钥在数据库创建后不能修改。
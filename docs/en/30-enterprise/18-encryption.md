---
toc_max_heading_level: 4
title: "Database encryption"
sidebar_label: "Database encryption"
---

## Introduction
Encrypt the data in database by the algorithm which was specified by the database admin.

## Config encryption key

### Config in offline mode
The encryption key need to be configured in every dnode. The configure is done as follow: 
```
taosd -y {encryptKey}
```
The characters in encryption key should be longer than 8, and shorter than 16. The characters is able to include Uppercase and lowercase letters, numbers, all special characters.

### Config in online mode
When all nodes is online, the encryption key configure is done as follow in taos shell:
```
create encrypt_key 'value'
```
The characters in encryption key should be longer than 8, and shorter than 16. The characters is able to include Uppercase and lowercase letters, numbers, all special characters.

## Create encrypted database
The encypted database is created when ENCRYPT_ALGORITHM is set in create database SQL. The detail grammar is as follow:
```
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
   ENCRYPT_ALGORITHM {'none' | 'sm4'}
}
```
sm4 means sm4 encryption algorithm is used.

## Alter database encryption
Changing ENCRYPT_ALGORITHM at existing database is not supported, including changing from non-encrypted to encrypted and from encrypted to non-encrypted.


## Chech database encryption configure
The database encryption configuration can be checked as follow:
```
select name, `encrypt_algorithm` from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

## Check dnode key status
The dnode key status can be checked as follow:
```
show encryptions;

select * from information_schema.ins_encryptions;
  dnode_id   |           key_status           |
===============================================
           1 | loaded                         |
           2 | unset                          |
           3 | unknown                        |
```
key_status mean：
- unset: when no encryption key is configured at this dnode.
- loaded: when the encryption key is configured and loaded at this dnode.
- unknown: when the dnode is not started, the key status is not available.

## Update encyption key
When the dnode hardware configuration is changed, the encryption key need to be updated. The encryption key is updated as follow，this operation is same as configuring encyption key in offline mode：
```
taosd -y  {encryptKey}
```

The taosd need to be shutdowned before updating encryption key. And the encryption key cannot be changed.
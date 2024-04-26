---
toc_max_heading_level: 4
title: "Database encryption"
sidebar_label: "Database encryption"
---

## Introduction
This section describes database encryption feature, which is available from TDengine Enterprise 3.3.0.0. With this feature, a database can be fully encrypted using the algorithm specified when creating the database. 

## Configure Encryption Key
To enable the database encryption feature, the system administrator needs to first configure the encryption key. There are two modes to do this, offline or online.

### Offline Mode
The encryption key needs to be configured for every dnode in offline mode, using the command below: 

```shell
taosd -y {encryptKey}
```
The characters in encryption key should be equal to or longer than 8, and shorter than 16. The characters can include uppercase and lowercase letters, numbers, and all special characters that are printable.

If one or more dnode are missed for configuring the encryption key, the database encryption feature is not available and you will get error when creating a database with encryption.

### Online Mode
If the system administrator doesn't configure the encryption key in offline mode and all dnode are already online, the encryption key can be configured using SQL command:

```sql
create encrypt_key 'value'
```
The limit for the encryption key is same as offline mode.

## Create Database with Encryption

You can create a database with encryption using the SQL commmand as below:

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
   ENCRYPT_ALGORITHM {'none' | 'sm4'}
}
```

You can see that `ENCRYPTION_ALGORITHM` is the key option for enabling encryption. For now, only `sm4` algorithm is supported. You need to configure either `sm4` or `none`, and `none` means actually no encryption. 

## Alter Encryption Algorithm

It's not allowed to change the encryption algorithm after a database is created.

## View Encryption Details

The database encryption configuration can be viewed as follow:

```sql
select name, `encrypt_algorithm` from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

## Check Encryption Key

The encryption key configrued properly or not for each dnode can be checked as below:

```sql
show encryptions;

select * from information_schema.ins_encryptions;
  dnode_id   |           key_status           |
===============================================
           1 | loaded                         |
           2 | unset                          |
           3 | unknown                        |
```
key_status means：
- unset: no encryption key is configured at this dnode.
- loaded: the encryption key is configured and loaded at this dnode.
- unknown: the dnode is not started, the key status is not available.

## Update Encyption Key

When the hardware of running a dnode is changed, the encryption key need to be updated. The encryption key is updated as follow，and the operation is same as configuring encyption key in offline mode：

```shell
taosd -y  {encryptKey}
```

The taosd process needs to be shutdown before updating encryption key. And the encryption key cannot be changed, that means you need to use the encryption key you used before.

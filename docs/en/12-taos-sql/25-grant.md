---
sidebar_label: Permissions Management
title: Permissions Management
---

This document describes how to manage permissions in TDengine.

## Create a User

```sql
CREATE USER use_name PASS 'password';
```

This statement creates a user account.

The maximum length of use_name is 23 bytes.

The maximum length of password is 128 bytes. The password can include leters, digits, and special characters excluding single quotation marks, double quotation marks, backticks, backslashes, and spaces. The password cannot be empty.

## Delete a User

```sql
DROP USER user_name;
```

## Modify User Information

```sql
ALTER USER user_name alter_user_clause
 
alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
}
```

- PASS: Modify the user password.
- ENABLE: Specify whether the user is enabled or disabled. 1 indicates enabled and 0 indicates disabled.
- SYSINFO: Specify whether the user can query system information. 1 indicates that the user can query system information and 0 indicates that the user cannot query system information.


## Grant Permissions

```sql
GRANT privileges ON priv_level TO user_name
 
privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.*
  | *.*
}
```

Grant permissions to a user.

Permissions are granted on the database level. You can grant read or write permissions.

TDengine has superusers and standard users. The default superuser name is root. This account has all permissions. You can use the superuser account to create standard users. With no permissions, standard users can create databases and have permissions on the databases that they create. These include deleting, modifying, querying, and writing to their own databases. Superusers can grant users permission to read and write other databases. However, standard users cannot delete or modify databases created by other users.

For non-database objects such as users, dnodes, and user-defined functions, standard users have read permissions only, generally by means of the SHOW statement. Standard users cannot create or modify these objects.

## Revoke Permissions

```sql
REVOKE privileges ON priv_level FROM user_name
 
privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.*
  | *.*
}

```

Revoke permissions from a user.

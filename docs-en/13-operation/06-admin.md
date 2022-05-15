---
title: User Management
---

System operator can use TDengine CLI `taos` to create or remove user or change password. The SQL command is as low:

## Create User

```sql
CREATE USER <user_name> PASS <'password'>;
```

When creating a user and specifying the user name and password, password needs to be quoted using single quotes.

## Drop User

```sql
DROP USER <user_name>;
```

Drop a user can only be performed by root.

## Change Password

```sql
ALTER USER <user_name> PASS <'password'>;
```

To keep the case of the password when changing password, password needs to be quoted using single quotes.

## Change Privilege

```sql
ALTER USER <user_name> PRIVILEGE <write|read>;
```

The privileges that can be changed to are `read` or `write` without single quotes.

Note：there is another privilege `super`, which not allowed to be authorized to any user.

## Show Users

```sql
SHOW USERS;
```

:::note
In SQL syntax, `< >` means the part that needs to be input by user, excluding the `< >` itself.

:::

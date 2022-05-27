---
title: User Management
---

A system operator can use TDengine CLI `taos` to create or remove users or change passwords. The SQL commands are documented below:

## Create User

```sql
CREATE USER <user_name> PASS <'password'>;
```

When creating a user and specifying the user name and password, the password needs to be quoted using single quotes.

## Drop User

```sql
DROP USER <user_name>;
```

Dropping a user can only be performed by root.

## Change Password

```sql
ALTER USER <user_name> PASS <'password'>;
```

To keep the case of the password when changing password, the password needs to be quoted using single quotes.

## Change Privilege

```sql
ALTER USER <user_name> PRIVILEGE <write|read>;
```

The privileges that can be changed to are `read` or `write` without single quotes.

Note：there is another privilege `super`, which is not allowed to be authorized to any user.

## Show Users

```sql
SHOW USERS;
```

:::note
In SQL syntax, `< >` means the part that needs to be input by the user, excluding the `< >` itself.

:::

---
title: 用户管理
---

系统管理员可以在 CLI 界面里添加、删除用户，也可以修改密码。CLI 里 SQL 语法如下：

```sql
CREATE USER <user_name> PASS <'password'>;
```

创建用户，并指定用户名和密码，密码需要用单引号引起来，单引号为英文半角

```sql
DROP USER <user_name>;
```

删除用户，限 root 用户使用

```sql
ALTER USER <user_name> PASS <'password'>;
```

修改用户密码，为避免被转换为小写，密码需要用单引号引用，单引号为英文半角

```sql
ALTER USER <user_name> PRIVILEGE <write|read>;
```

修改用户权限为：write 或 read，不需要添加单引号

说明：系统内共有 super/write/read 三种权限级别，但目前不允许通过 alter 指令把 super 权限赋予用户。

```sql
SHOW USERS;
```

显示所有用户

:::note
SQL 语法中，< >表示需要用户输入的部分，但请不要输入< >本身。

:::

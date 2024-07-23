---
sidebar_label: 用户管理
title: TDengine 用户管理
toc_max_heading_level: 4
---

TDengine 默认仅配置了一个 root 用户，该用户拥有最高权限。

## 创建用户

创建用户的操作只能由 root 用户进行，语法如下。
```sql
create user user_name pass'password' [sysinfo {1|0}]
```

相关参数说明如下。
- user_name：最长为 23 B。
- password：最长为 128 B，合法字符包括 a~z、A~Z、0~9、!、?、\、$、%、\、^、&、*、( )、_、–、+、=、{、[、}、]、:、;、@、~、#、|、\、<、,、>、.、?、/、，不可以出现单双引号、撇号、反斜杠和空格，且不可以为空。
- sysinfo ：用户是否可以查看系统信息。1 表示可以查看，0 表示不可以查看。系统信息包括服务端配置信息、服务端各种节点信息，如 dnode、查询节点（qnode）等，以及与存储相关的信息等。默认为可以查看系统信息。

如下 SQL 可以创建密码为 123456 且可以查看系统信息的用户 test。

```sql
create user test pass '123456' sysinfo 1
```

## 查看用户

查看系统中的用户信息可使用如下 SQL。
```sql
show users;
```

也可以通过查询系统表 information_schema.ins_users 获取系统中的用户信息，示例如下。
```sql
select * from information_schema.ins_users;
```

## 修改用户信息

修改用户信息的 SQL 如下。
```sql
alter user user_name alter_user_clause 
alter_user_clause: { 
 pass 'literal' 
 | enable value 
 | sysinfo value
}
```

相关参数说明如下。
- pass：修改用户密码。
- enable：是否启用用户。1 表示启用此用户，0 表示禁用此用户。
- sysinfo ：用户是否可查看系统信息。1 表示可以查看系统信息，0 表示不可以查看系统信息

如下 SQL 禁用 test 用户。
```sql
alter user test enable 0
```

## 删除用户

删除用户的 SQL 如下。
```sql
drop user user_name
```
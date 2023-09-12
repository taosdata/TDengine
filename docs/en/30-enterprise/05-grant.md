---
toc_max_heading_level: 4
title: Permissions Management
---

Access control in TDengine includes user management, database authorization management, and message subscription authorization.


After a successful installation and deployment of TDengine, the system comes with a built-in "root" user. System administrators with the default "root" user password should change the root user's password immediately and create ordinary users and grant them appropriate permissions as needed for business purposes. Without authorization, ordinary users can create databases and have full permissions over the databases they create, including deleting databases, modifying databases, querying time-series data, and writing time-series data. Superusers can grant read and write permissions to ordinary users on other databases (i.e., databases not created by the user), allowing them to read and write data on these databases but not perform operations like deleting and modifying databases. Superusers or the creators of topics can also grant subscription permissions to other users for a specific topic.

## User Management

User management involves the entire lifecycle of a user, from creating a user, authorizing a user, revoking authorizations, viewing user information, to deleting a user.

### Create a User

Only the root user can create users using the following syntax:

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1\|0}]; 
```

Description:

- `user_name` can be up to 23 bytes long.
- `password` can be up to 128 bytes long and can include valid characters such as "a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/", but it cannot contain single or double quotes, backticks, or spaces, and it cannot be empty.
- SYSINFO indicates whether the user can view system information. 1 means they can view it, and 0 means they cannot. System information includes server configuration, various node information (such as DNODE, QNODE, etc.), and storage-related information. The default is to allow viewing system information.

Example: Creating a user named "test" with the password "123456" and the ability to view system information:

```
SQL taos\> create user test pass '123456' sysinfo 1; Query OK, 0 of 0 rows affected (0.001254s)
```

### View Users

To view information about users in the system, use the show users command, as shown below:

```sql
show users;
```

You can also retrieve user information from the system table INFORMATION_SCHEMA.INS_USERS, as demonstrated below:

```sql
select * from information_schema.ins_users;  
```

### Delete a User

Delete a User

```sql
DROP USER user_name; 
```

### Modify User Information

Modify User Information

```sql
ALTER USER user_name alter_user_clause   alter_user_clause: {  PASS 'literal'  \| ENABLE value  \| SYSINFO value } 
```

Description:

- PASS: Modify the user password.
- ENABLE: Specify whether the user is enabled or disabled. 1 indicates enabled and 0 indicates disabled.
- SYSINFO: Specify whether the user can query system information. 1 indicates that the user can query system information and 0 indicates that the user cannot query system information.

Example: delete test user

```sql
alter user test enable 0; Query OK, 0 of 0 rows affected (0.001160s) 
```

### Database Access Control

System administrators can grant specific authorizations to each user for each database in the system, depending on business needs, to prevent unauthorized access or modifications to business data. The syntax for granting database access to a user is as follows:

```sql
GRANT privileges ON priv_level TO user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  READ  \| WRITE }   priv_level : {  dbname.\*  \| \*.\* } 
```

Database access permissions include read and write permissions, which can be granted separately or together.

Description:

In the priv_level format, the part before the "." is the database name, and the part after the "." is the table name. However, table-level authorization control is not supported at this time, so the part after the "." must be "\*" indicating all tables in the database specified before the ".".
"dbname.\*" means all tables in the database named "dbname."
"\*.\*" means all tables in all database names.

### Database Permissions

The permissions for the root user and ordinary users are explained in the table below:

| User     | Description                               | Permissions                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Superuser	| Only "root" is a superuser	| All operations outside of the DB, such as CRUD DB permissions for users, dnode, udf, qnode, etc. DB permissions include creating, deleting, and updating. For example, modifying options, moving Vgroups, enabling/disabling users. Read and write Enable/Disable users |
| Ordinary User	| All users except "root" are ordinary users	| In a readable DB, ordinary users can perform read operations such as select, describe, show, and subscribe. In a writable DB, users can perform write operations, including creating, deleting, and modifying super tables, creating, deleting, and modifying sub-tables, creating, deleting, and modifying topics, and writing data. When restricted from viewing system information, users cannot perform operations such as show dnode, mnode, vgroups, qnode, snode, modify users, including their own passwords, and when they use "show db," they can only see their own databases without seeing vgroups, replicas, caches, and other information. Regardless of whether they are restricted from viewing system information, they can manage UDFs and create databases. They have full permissions on databases they create and have read and write permissions on databases they didn't create, according to the read and write lists.

Message Subscription Authorization

Any user can create topics on databases they have read access to. The superuser "root" can create topics on any database. Subscription permissions for each topic can be independently granted to any user, regardless of whether they have access to the database. Only the root user or the creator of a topic can delete it. Topics can be subscribed to by superusers, the creators of topics, or users explicitly granted "subscribe" permissions.

The syntax for granting subscription permissions is as follows:

```sql
GRANT privileges ON priv_level TO user_name  privileges : {  ALL  | priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topic_name } 
```

### Tag-Based Authorization (Table-Level Authorization)

Starting from TDengine 3.0.5.0, we support authorizing specific sub-tables within a super table using tag-based authorization. The SQL syntax for this is as follows:

```sql
GRANT privileges ON priv_level [WITH tag_condition] TO user_name
 
privileges : {
    ALL
  | SUBSCRIBE
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.tbname
  | dbname.*
  | *.*
  | topic_name
}

REVOKE privileges ON priv_level [WITH tag_condition] FROM user_name

privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.tbname
  | dbname.*
  | *.*
}
```

The semantics of the above SQL are as follows:

- Users can grant or revoke read and write permissions for specified tables (including super tables and regular tables) using dbname.tbname. Directly granting or revoking permissions for sub-tables is not supported.
- Users can grant or revoke read and write permissions for all sub-tables that meet specific conditions using dbname.tbname and the WITH clause. When using the WITH clause, the permission level must be for a super table.

### Relationship Between Table-Level Authorization and Database Authorization

The table below outlines the actual permissions resulting from different combinations of database authorization and table-level authorization:

|                |**No Table Permissions**       | **Table Read Permissions** | **Table Read Permissions with Tag Conditions** | **Table Write Permissions** | **Table Write Permissions with Tag Conditions** |
| -------------- | ---------------- | -------- | ---------- | ------ | ----------- | 
No Database Authorization	| No authorization	| Read permission for this table, no permission for other tables under the database	| Read permission for sub-tables under this table that meet the tag condition, no permission for other tables under the database	| Write permission for this table, no permission for other tables under the database	| Write permission for sub-tables under this table that meet the tag condition, no permission for other tables under the database | 
No Database Authorization	| No authorization	| Read permission for this table, no permission for other tables under the database	| Read permission for sub-tables under this table that meet the tag condition, no permission for other tables under the database	| Write permission for this table, no permission for other tables under the database	| Write permission for sub-tables under this table that meet the tag condition, no permission for other tables under the database | 
Read Database Authorization	| Read permission for all tables	| Read permission for all tables	| Read permission for sub-tables under this table that meet the tag condition, read permission for all tables under the database	| Write permission for this table, read permission for all tables	| Write permission for sub-tables under this table that meet the tag condition, read permission for all tables under the database | 


### View User Permissions

You can display the authorizations a user has using the following command:

```sql
show user privileges 
```

### Revoke Permissions

1. Revoke database access authorization

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  READ  \| WRITE }   priv_level : {  dbname.\*  \| \*.\* }  
```

2. Revoke data subscription authorization

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topi_name } 
```
---
toc_max_heading_level: 4
title: Permissions Management
---

TDengine offers permissions management for users, databases, and data subscription.

When TDengine is installed, a root user is created. For security purposes, you must change the password of the root user after installation has completed. You can then create other users with defined permissions appropriate for their roles. If permissions for a user have not been defined, normal users can create databases and have full permissions for the databases that they create. This includes deleting and modifying the database as well as inserting data into and querying data from the database. The root user can grant additional permissions to normal users such that they can insert data into or query data from databases that they do not own. However, normal users cannot be granted permission to delete or modify databases that they do not own. The root user can also grant normal users permission to subscribe to topics that they do not own.

## User Management

You can create and delete users, view their information, and grant and revoke permissions.

### Creating Users

To create a user, log in to TDengine as the root user and run the following SQL statement:

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1\|0}]; 
```

Notes:

- user_name: Enter a maximum of 23 characters.
- password: Enter a maximum of 128 characters, including letters, digits, and special characters. Passwords cannot contain single quotation marks ('), double quotation marks ("), backticks (`), backslashes (\), or spaces. Passwords cannot be empty.
- SYSINFO: Specify whether the user can view system information. Enter 1 to allow to user to view system information or 0 to prevent the user from viewing system information. System information includes server configuration, node information, and storage information. The default value is 1.

Example: The following SQL statement creates a user named `test` with password `123456` who can view system information:

```SQL
taos> CREATE USER test PASS '123456' SYSINFO 1;
Create OK, 0 row(s) affected (0.001254s)
```

### Query Users

You can use the SHOW USERS statement to view information about all users in your cluster.

```sql
show users;
```

You can also query the `INS_USERS` table in the `INFORMATION_SCHEMA` database to view user information.

```sql
select * from information_schema.ins_users;  
```

### Delete Users

You can delete users by running the following SQL statement:

```sql
DROP USER user_name; 
```

### Modify Users

You can modify user information by running the following SQL statement:

```sql
ALTER USER user_name alter_user_clause   alter_user_clause: {  PASS 'literal'  \| ENABLE value  \| SYSINFO value } 
```

Notes:

- PASS: Modify the user's password.
- ENABLE: Specify whether the user is enabled or disabled. Enter 1 to enable the user or 0 to disable the user.
- SYSINFO: Specify whether the user can view system information. Enter 1 to allow the user to view system information or 0 to prevent the user from viewing system information.

Example; The following SQL statement disables the `test` user:

```sql
alter user test enable 0;
Query OK, 0 row(s) affected (0.001160s) 
```

## Database Permissions

You can grant permissions to normal users so that they can perform operations on databases that they do not own. The following SQL statement grants database permissions:

```sql
GRANT {ALL | READ | WRITE} ON database_name TO user_name; 
```

You can specify read and write permissions for a database separately or in a single statement.

Notes:



- To grant permissions to all databases, enter an asterisk (*) for the database name.

### Description

Database permissions for root and normal users are described in the following table.

| User     | Description                               | Permissions                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Superuser | root               |  All operations                                                                                                                                                                                                                                                                                                                                     |
| Normal user | All users except root | On databases with read permissions, SELECT, DESCRIBE, SHOW, and SUBSCRIBE operations. On databases with write permissions, creating, modifying, and deleting supertables, subtables, and topics. Without sysinfo permissions, normal users cannot perform SHOW operations on dnodes, mnodes, vgroups, qnodes, or snodes. Users can change their own passwords. Users can show databases that they own, but system information such as vgroups, replicas, and cache is not displayed. Users can manage UDFs and create databases.

### Data Subscription Permissions

All users can create topics in the databases that they own. The root user can create topics in any database. You can grant topic permissions to any user, even if the user does not have permission to access the database in which the topic was created. Topics can be deleted only by the topic owner and the root user. Topics can be subscribed to by the root user, the topic owner, and users who have been given explicit permission.

The following SQL statement grants topic permissions:

```sql
GRANT {ALL | SUBSCRIBE} ON topic_name TO user_name; 
```

### Tag-based Permissions

You can grant permissions to a subset of subtables in a supertable based on the tags assigned to the subtables. The following SQL statements grant and revoke tag-based permissions:

```sql
GRANT {ALL | READ | WRITE} ON dbname.tbname WITH tag_condition TO user_name;
 

    
  
  

 

    
  

 

    
  
  
  


REVOKE {ALL | READ | WRITE} ON dbname.tbname WITH tag_condition FROM user_name;


    
  

 

    
  

 

    
  
  

```

This statement is described as follows:

- You must specify a supertable when granting or revoking tag-based permissions. You cannot specify a standard table or subtable.
 In the WITH clause, specify the column for which you want to grant permissions.

### Table and Database Permissions

The following table describes the effects of granting database and table permissions to users.

|                |**No Table Permissions**       | **Table Read Permissions** | **Table Read Permissions with Tag Condition** | **Table Write Permissions** | **Table Write Permissions with Tag Condition** |
| -------------- | ---------------- | -------- | ---------- | ------ | ----------- | 
| **No Database Permissions**  | None          | Read permissions on the specified table only   |  Read permissions on subtables matching the specified tag conditions within the specified supertable       | Write permissions on the specified table only      | Write permissions on subtables matching the specified tag conditions within the specified supertable      | 
| **Database Read Permissions**  | Read permissions on all tables | Read permissions on all tables     | Read permissions on subtables matching the specified tag conditions within the specified supertable and read permissions on all other tables       | Write permissions on the specified table and read permissions on all tables    | Write permissions on subtables matching the specified tag conditions within the specified supertable and read permissions on all tables | 
| **Database Write Permissions**  | Write permissions on all tables | Read permissions on the specified table and write permissions on all tables    | Read permissions on subtables matching the specified tag conditions within the specified supertable and write permissions on all tables      | Write permissions on all tables     | Write permissions on subtables matching the specified tag conditions within the specified supertable and write permissions on all other tables       | 


### User Permissions

You can use the following SQL statement to view the permissions granted to all users:

```sql
show user privileges 
```

### Revoking Permissions

1. You can use the following SQL statement to revoke database permissions:

```sql
REVOKE {ALL | READ | WRITE} ON db_name FROM user_name;  
```

2. You can use the following SQL statement to revoke topic permissions:

```sql
REVOKE {ALL | READ | WRITE} ON topic_name FROM user_name; 
```
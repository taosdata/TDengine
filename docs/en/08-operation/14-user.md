---
title: Users and Permissions
slug: /operations-and-maintenance/manage-users-and-permissions
---

TDengine is configured by default with only one root user, who has the highest permissions. TDengine supports access control for system resources, databases, tables, views, and topics. The root user can set different access permissions for each user for different resources. This section introduces user and permission management in TDengine. User and permission management is a feature unique to TDengine Enterprise.

## User Management

### Creating Users

Only the root user can perform the operation of creating users, with the syntax as follows.

```sql
create user user_name pass'password' [sysinfo {1|0}] [createdb {1|0}]
```

The parameters are explained as follows.

- user_name: Up to 23 B long.
- password: The password must be between 8 and 255 characters long. The password include at least three types of characters from the following: uppercase letters, lowercase letters, numbers, and special characters, special characters include `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`, and this requirement is able to be closed by adding enableStrongPassword 0 in taos.cfg, or by the following SQL:

```sql
alter all dnodes 'EnableStrongPassword' '0'
```

- sysinfo: Whether the user can view system information. 1 means they can view it, 0 means they cannot. System information includes server configuration information, various node information such as dnode, query node (qnode), etc., as well as storage-related information, etc. The default is to view system information.
- createdb: Whether the user can create databases. 1 means they can create databases, 0 means they cannot. The default value is 0. // Supported starting from TDengine Enterprise version 3.3.2.0

The following SQL can create a user named test with the password abc123!@# who can view system information.

```sql
create user test pass 'abc123!@#' sysinfo 1
```

### Viewing Users

To view user information in the system, use the following SQL.

```sql
show users;
```

You can also obtain user information in the system by querying the system table information_schema.ins_users, as shown below.

```sql
select * from information_schema.ins_users;
```

### Modifying User Information

The SQL for modifying user information is as follows.

```sql
alter user user_name alter_user_clause 
alter_user_clause: { 
 pass 'literal' 
 | enable value 
 | sysinfo value
 | createdb value
}
```

The parameters are explained as follows.

- pass: Modify the user's password.
- enable: Whether to enable the user. 1 means to enable this user, 0 means to disable this user.
- sysinfo: Whether the user can view system information. 1 means they can view system information, 0 means they cannot.
- createdb: Whether the user can create databases. 1 means they can create databases, 0 means they cannot. // Supported starting from TDengine Enterprise version 3.3.2.0

The following SQL disables the user test.

```sql
alter user test enable 0
```

### Deleting Users

The SQL for deleting a user is as follows.

```sql
drop user user_name
```

## Permission Management

Only the root user can manage system information such as users, nodes, vnode, qnode, snode, including querying, adding, deleting, and modifying.

### Granting Permissions to Databases and Tables

In TDengine, permissions for databases and tables are divided into read (read) and write (write) types. These permissions can be granted individually or together to users.

- read permission: Users with read permission can only query data in the database or table, but cannot modify or delete data. This type of permission is suitable for scenarios where access to data is needed but no write operations are required, such as data analysts, report generators, etc.
- write permission: Users with write permission can write data to the database or table. This type of permission is suitable for scenarios where data writing operations are needed, such as data collectors, data processors, etc. If you only have write permission without read permission, you can only write data but cannot query data.

The syntax for granting a user access to databases and tables is as follows.

```sql
grant privileges on resources [with tag_filter] to user_name
privileges: {
 all,
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
}
resources: {
 dbname.tbname
 | dbname.*
 | *.*
}
```

The parameters are explained as follows.

- resources: The databases or tables that can be accessed. The part before the dot is the database name, and the part after the dot is the table name. dbname.tbname means the table named tbname in the database named dbname, which must be a basic table or supertable. dbname.* means all tables in the database named dbname. *.* means all tables in all databases.
- tag_filter: Filter condition for supertables.

The above SQL can authorize a database, all databases, a regular table or a supertable under a database, and it can also authorize all subtables under a supertable that meet filter conditions through a combination of dbname.tbname and the with clause.
The following SQL grants read permission on the database power to the user test.

```sql
grant read on power to test
```

The following SQL grants all permissions on the supertable meters under the database power to the user test.

```sql
grant all on power.meters to test
```

The following SQL grants write permission to the user test for subtables of the supertable meters where the tag value groupId equals 1.

```sql
grant all on power.meters with groupId=1 to test
```

If a user is granted write permission to a database, then the user has both read and write permissions for all tables under this database. However, if a database has only read permission or even no read permission, table authorization allows the user to read or write some tables. See the reference manual for detailed authorization combinations.

### View Authorization

In TDengine, view permissions are divided into read, write, and alter. These determine the user's access and operation permissions on the view. Here are the specific rules for view permissions:

- The creator of the view and root users have all permissions by default. This means that the creator of the view and root users can query, write to, and modify the view.
- Granting and revoking permissions to other users can be done through grant and revoke statements. These operations can only be performed by root users.
- View permissions need to be granted and revoked separately; authorizations and revocations through db.* do not include view permissions.
- Views can be nested in definition and use, and the verification of view permissions is also done recursively.

To facilitate the sharing and use of views, TDengine introduces the concept of effective users of views (i.e., the creator of the view). Authorized users can use the read and write permissions of the effective user's databases, tables, and nested views. When a view is replaced, the effective user is also updated.

For detailed relationships between view operations and permission requirements, see the reference manual.

The syntax for view authorization is as follows.

```sql
grant privileges on [db_name.]view_name to user_name
privileges: {
 all,
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
 alter
}
```

To grant read permission on the view view_name under the database power to the user test, the SQL is as follows.

```sql
grant read on power.view_name to test
```

To grant all permissions on the view view_name under the database power to the user test, the SQL is as follows.

```sql
grant all on power.view_name to test
```

### Message Subscription Authorization

Message subscription is a unique design of TDengine. To ensure the security of user subscription information, TDengine can authorize message subscriptions. Before using the message subscription authorization feature, users need to understand the following special usage rules:

- Any user with read permission on a database can create a topic. Root users have the permission to create topics on any database.
- Each topic's subscription permission can be independently granted to any user, regardless of whether they have access permission to that database.
- Only root users or the creator of the topic can perform the deletion of a topic.
- Only superusers, the creator of the topic, or users explicitly granted subscription permission can subscribe to the topic. These permission settings ensure the security of the database while also allowing users flexibility within a limited range.

The SQL syntax for message subscription authorization is as follows.

```sql
grant privileges on priv_level to user_name 
privileges : { 
 all 
 | priv_type [, priv_type] ...
} 
priv_type : { 
 subscribe
} 
priv_level : { 
 topic_name
}
```

To grant subscription permission on the topic named topic_name to the user test, the SQL is as follows.

```sql
grant subscribe on topic_name to test
```

### Viewing Authorizations

When a company has multiple database users, the following command can be used to query all the authorizations a specific user has, the SQL is as follows.

```sql
show user privileges
```

### Revoking Authorizations

Due to the different characteristics of database access, data subscription, and views, the syntax for revoking specific authorizations also varies slightly. Below are the specific revocation authorization syntaxes for different authorization objects.
The SQL for revoking database access authorization is as follows.

```sql
revoke privileges on priv_level [with tag_condition] from user_name
privileges : {
 all
 | priv_type [, priv_type] ...
}
priv_type : {
 read
 | write
}
priv_level : {
 dbname.tbname
 | dbname.*
 | *.*
}
```

The SQL for revoking view permissions is as follows.

```sql
revoke privileges on [db_name.]view_name from user_name
privileges: {
 all,
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
 | alter
}
```

The SQL for revoking data subscription permissions is as follows.

```sql
revoke privileges on priv_level from user_name 
privileges : {
    all 
 | priv_type [, priv_type] ...
} 
priv_type : { 
 subscribe
} 
priv_level : { 
 topic_name
}
```

The SQL for revoking all permissions of user test on the database power is as follows.

```sql
revoke all on power from test
```

The SQL for revoking the read permission of user test on the view view_name of the database power is as follows.

```sql
revoke read on power.view_name from test
```

The SQL for revoking the subscribe permission of user test on the message subscription topic_name is as follows.

```sql
revoke subscribe on topic_name from test
```

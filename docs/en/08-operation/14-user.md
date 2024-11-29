---
title: Manage Users and Permissions
slug: /operations-and-maintenance/manage-users-and-permissions
---

TDengine is configured by default with only one root user, who has the highest privileges. TDengine supports access control for system resources, databases, tables, views, and topics. The root user can set different access permissions for each user based on different resources. This section introduces user and permission management in TDengine. User and permission management is a feature unique to TDengine Enterprise.

## User Management

### Creating Users

Only the root user can create users, with the syntax as follows.

```sql
create user user_name pass'password' [sysinfo {1|0}]
```

The related parameters are described as follows.

- user_name: Maximum length is 23 bytes.
- password: Maximum length is 128 bytes; valid characters include letters, numbers, special characters other than single and double quotes, apostrophes, backslashes, and spaces, and it cannot be empty.
- sysinfo: Whether the user can view system information. 1 means the user can view it, while 0 means the user cannot view it. System information includes server configuration information, various node information such as dnode and query nodes (qnode), and storage-related information. The default is to allow viewing of system information.

The following SQL creates a user named test with a password of 123456, who can view system information.

```sql
create user test pass '123456' sysinfo 1
```

### Viewing Users

To view user information in the system, you can use the following SQL.

```sql
show users;
```

You can also query user information in the system table `information_schema.ins_users`, as shown below.

```sql
select * from information_schema.ins_users;
```

### Modifying User Information

The SQL to modify user information is as follows.

```sql
alter user user_name alter_user_clause 
alter_user_clause: { 
 pass 'literal' 
 | enable value 
 | sysinfo value
}
```

The related parameters are described as follows.

- pass: Modify the user's password.
- enable: Whether to enable the user. 1 means to enable this user, 0 means to disable this user.
- sysinfo: Whether the user can view system information. 1 means the user can view system information, 0 means the user cannot view system information.

The following SQL disables the test user.

```sql
alter user test enable 0
```

### Deleting Users

The SQL to delete a user is as follows.

```sql
drop user user_name
```

## Permission Management

Only the root user can manage users, nodes, vnodes, qnodes, snodes, and other system information, including querying, adding, deleting, and modifying.

### Granting Permissions for Databases and Tables

In TDengine, permissions for databases and tables are divided into read and write. These permissions can be granted individually or simultaneously to users.

- Read Permission: A user with read permission can only query data in the database or table but cannot modify or delete it. This permission is suitable for scenarios where access to data is needed but no write operations are required, such as data analysts or report generators.
- Write Permission: A user with write permission can write data to the database or table. This permission is suitable for scenarios that require writing operations, such as data collectors or processors. If a user has only write permission without read permission, they can only write data but cannot query it.

The syntax for granting access to a user for a specific database and table is as follows.

```sql
grant privileges on resources [with tag_filter] to user_name
privileges: {
 all,
 | priv_type [, priv_type] …
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

The related parameters are described as follows.

- resources: The databases or tables that can be accessed. The part before the dot is the database name, and the part after the dot is the table name. `dbname.tbname` means the table `tbname` in the database `dbname` must be a basic table or a supertable. `dbname.*` means all tables in the database `dbname`. `*.*` means all tables in all databases.
- tag_filter: Filtering conditions for supertables.

The above SQL can grant access to a single database, all databases, a basic table or supertable under a database, or all subtables of a supertable that meet the filtering conditions using a combination of `dbname.tbname` and the `with` clause.
The following SQL grants read permission for the database `power` to the user `test`.

```sql
grant read on power to test
```

The following SQL grants all permissions on the supertable `meters` in the database `power` to the user `test`.

```sql
grant all on power.meters to test
```

The following SQL grants write permission for the subtable of the supertable `meters` with the label value `groupId` equal to 1 to the user `test`.

```sql
grant all on power.meters with groupId=1 to test
```

If a user is granted write permission for a database, they will have both read and write permissions for all tables under that database. However, if a database only has read permission or even no read permission, table permissions will allow the user to read or write to specific tables. Detailed permission combinations can be found in the reference manual.

### Granting Permissions for Views

In TDengine, view permissions are divided into read, write, and alter. These permissions determine a user's access and operational authority over the views. The specific rules for using view permissions are as follows:

- The creator of the view and the root user have all permissions by default. This means the creator of the view and the root user can query, write, and modify the view.
- Granting and revoking permissions for other users can be done using the `grant` and `revoke` statements. These operations can only be executed by the root user.
- View permissions need to be granted and revoked separately; granting and revoking using `db.*` does not include view permissions.
- Views can be defined and used in nested forms, and permission checks for views are performed recursively.

To facilitate sharing and usage of views, TDengine introduces the concept of valid users for views (i.e., the user who created the view). Authorized users can use the read and write permissions of the valid user's databases, tables, and nested views. When a view is replaced, the valid user is also updated.

For detailed mappings of view operations and permission requirements, please refer to the reference manual.

The syntax for granting view permissions is as follows.

```sql
grant privileges on [db_name.]view_name to user_name
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

The SQL below grants read permission on the view `view_name` in the database `power` to the user `test`.

```sql
grant read on power.view_name to test
```

The SQL below grants all permissions on the view `view_name` in the database `power` to the user `test`.

```sql
grant all on power.view_name to test
```

### Granting Permissions for Message Subscriptions

Message subscription is a unique design in TDengine. To ensure the security of user subscription information, TDengine can grant permissions for message subscriptions. Before using the message subscription permission feature, users need to understand the following special usage rules:

- Any user with read permission on a database can create topics. The root user has the permission to create topics on any database.
- Subscription permissions for each topic can be independently granted to any user, regardless of whether they have access to the database.
- Only the root user or the creator of the topic can delete the topic.
- Only superusers, the topic creator, or users explicitly granted subscription permissions can subscribe to the topic. These permission settings ensure both the security of the database and flexible operations for users within a limited scope.

The SQL syntax for granting message subscription permissions is as follows.

```sql
grant privileges on priv_level to user_name 
privileges: { 
 all 
 | priv_type [, priv_type] ...
} 
priv_type: { 
 subscribe
} 
priv_level: { 
 topic_name
}
```

The following SQL grants the topic named `topic_name` to the user `test`.

```sql
grant subscribe on topic_name to test
```

### Viewing Grants

When a company has multiple database users, you can use the following command to query all grants held by a specific user, as shown in the SQL below.

```sql
show user privileges
```

### Revoking Grants

Due to the different characteristics of database access, data subscriptions, and views, the syntax for revoking specific grants varies slightly. Below are the specific revocation syntax corresponding to different grant objects.
The SQL for revoking database access grants is as follows.

```sql
revoke privileges on priv_level [with tag_condition] from user_name
privileges: {
 all
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
}
priv_level: {
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
privileges: {
    all 
 | priv_type [, priv_type] ...
} 
priv_type: { 
 subscribe
} 
priv_level: { 
 topic_name
}
```

The SQL below revokes all grants for the user `test` on the database `power`.

```sql
revoke all on power from test
```

The SQL below revokes read permission for the user `test` on the view `view_name` in the database `power`.

```sql
revoke read on power.view_name from test
```

The SQL below revokes the subscribe permission for the user `test` on the message subscription `topic_name`.

```sql
revoke subscribe on topic_name from test
```

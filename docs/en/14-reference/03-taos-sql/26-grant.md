---
title: Manage Permissions
slug: /tdengine-reference/sql-manual/manage-permissions
---

Permission management in TDengine consists of [user management](../manage-users/), database authorization management, and message subscription authorization management. This section focuses on database authorization and subscription authorization.

## Database Access Authorization

System administrators can grant specific permissions to each user for each database based on business needs to prevent improper access to business data. The syntax for granting database access authorization to a user is as follows:

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
    dbname.tbname
  | dbname.*
  | *.*
}
```

Database access permissions include read and write permissions, which can be granted separately or together.

**Explanation**

- In the `priv_level` format, the part before the "." is the database name, and the part after the "." is the table name, indicating table-level authorization control. If the part after the "." is "*", it means all tables in the database specified before the ".".
- "dbname.*" means all tables in the database named "dbname".
- "*.*" means all tables in all database names.

### Database Permission Description

The permissions for the root user and ordinary users are described in the table below:

| User        | Description                          | Permission Description                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-------------|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Super User  | Only root is a super user           | All operation permissions outside of DB, such as CRUD for users, dnodes, UDFs, qnodes, etc. DB permissions include creating, deleting, updating (e.g., modifying options, moving vgroups, etc.). Read, write, enable/disable users.                                                                                                                                                                                                                                                  |
| Ordinary User | All users except root are ordinary users | In readable DBs, ordinary users can perform read operations such as select, describe, show, and subscribe. In writable DBs, users can perform write operations: create, delete, modify supertables, create, delete, modify subtables, create, delete, modify topics, and write data. When system information is restricted, the following operations cannot be performed: show dnodes, mnodes, vgroups, qnodes, snodes, modifying users including their own passwords, and when showing DBs, they can only see their own DB without seeing vgroups, replicas, cache, etc. Regardless of system information restrictions, they can manage UDFs and create their own DBs with full permissions, while non-created DBs are subject to the permissions in the read and write list. |

## Message Subscription Authorization

Any user can create a topic on a database they have read permissions for. The super user root can create topics on any database. The subscription permissions for each topic can be independently granted to any user, regardless of whether that user has access to the database. Only the root user or the creator of the topic can delete it. Topics can only be subscribed to by the super user, the topic's creator, or users explicitly granted subscribe permissions.

The specific SQL syntax is as follows:

```sql
GRANT SUBSCRIBE ON topic_name TO user_name

REVOKE SUBSCRIBE ON topic_name FROM user_name
```

## Tag-Based Authorization (Table-Level Authorization)

Starting from TDengine version 3.0.5.0, we support granting authorization for specific subtables within a supertable by tags. The specific SQL syntax is as follows.

```sql
GRANT privileges ON priv_level [WITH tag_condition] TO user_name
 
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

The semantics of the above SQL are:

- Users can grant or revoke read and write permissions for a specified table (including supertables and basic tables) through `dbname.tbname`. Direct grants or revokes of permissions on subtables are not supported.
- Users can grant or revoke read and write permissions for all subtables that meet the conditions through `dbname.tbname` and the WITH clause. When using the WITH clause, the permission level must be for the supertable.

## Relationship Between Table-Level Permissions and Database Permissions

The table below lists the actual permissions generated under different combinations of database authorization and table-level authorization.

|                  | **No Table Authorization** | **Table Read Authorization**                     | **Table Read Authorization with Tag Condition**                               | **Table Write Authorization**                 | **Table Write Authorization with Tag Condition**                       |
|------------------|----------------------------|-------------------------------------------------|---------------------------------------------------------------------------|------------------------------------------------|-----------------------------------------------------------------------|
| **No Database Authorization** | No authorization          | Read permission for this table; no permissions for other tables in the database | Read permission for subtables that meet tag permissions; no permissions for other tables in the database | Write permission for this table; no permissions for other tables in the database | Write permission for subtables that meet tag permissions; no permissions for other tables in the database |
| **Database Read Authorization** | Read permissions for all tables | Read permissions for all tables                    | Read permissions for subtables that meet tag permissions; read permissions for other tables in the database | Write permission for this table; read permissions for all tables | Write permission for subtables that meet tag permissions; read permissions for all tables |
| **Database Write Authorization** | Write permission for all tables | Read permission for this table; write permission for all tables | Read permission for subtables that meet tag permissions; write permission for all tables | Write permission for all tables                  | Write permission for subtables that meet tag permissions; write permission for other tables in the database |

## View User Authorizations

The following command can be used to display the authorizations a user has:

```sql
SHOW USER PRIVILEGES
```

## Revoke Authorization

1. Revoke database access authorization

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
    dbname.tbname
  | dbname.*
  | *.*
}
```

2. Revoke message subscription authorization

```sql
REVOKE privileges ON priv_level FROM user_name

privileges : {
    ALL
  | priv_type [, priv_type] ...
}

priv_type : {
    SUBSCRIBE
}

priv_level : {
    topic_name
}
```

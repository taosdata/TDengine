---
title: Permissions
slug: /tdengine-reference/sql-manual/manage-permissions
---

In TDengine, permission management is divided into [user management](../manage-users/), database authorization management, and message subscription authorization management. This section focuses on database authorization and subscription authorization.

## Database Access Authorization

System administrators can authorize each user in the system for each database according to business needs to prevent business data from being read or modified by inappropriate users. The syntax for authorizing a user for database access is as follows:

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

Database access permissions include read and write permissions, which can be granted separately or simultaneously.

:::note

- In the priv_level format, the "." before represents the database name, and the "." after represents the table name, meaning table-level authorization control. If "*" follows the ".", it means all tables in the database specified before the "."
- "dbname.*" means all tables in the database named "dbname"
- "*.*" means all tables in all database names

:::

### Database Permission Description

The permissions for root users and ordinary users are described in the following table

| User      | Description                        | Permission Description                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Superuser | Only root is a superuser           | All operations outside DB, such as CRUD for user, dnode, udf, qnode, etc. DB permissions, including create, delete, update, such as modifying Option, moving Vgroup, etc. Read, write, Enable/Disable users                                                                                                                                                                                                                                                                         |
| Ordinary User | All other users except root are ordinary users | In readable DBs, ordinary users can perform read operations: select, describe, show, subscribe In writable DBs, users can perform write operations: create, delete, modify supertables, create, delete, modify subtables, create, delete, modify topics, write data When restricted from system information, the following operations cannot be performed: show dnode, mnode, vgroups, qnode, snode Modify users including own password When showing db, can only see their own db, and cannot see vgroups, replicas, cache, etc. Regardless of whether system information is restricted, can manage udf Can create DB Own created DBs have all permissions Non-self-created DBs, refer to the permissions in the read, write list |

## Message Subscription Authorization

Any user can create a topic on a database where they have read permissions. The superuser root can create a topic on any database. Subscription permissions for each topic can be independently granted to any user, regardless of whether they have access permissions to the database. Deleting a topic can only be done by the root user or the creator of the topic. A topic can only be subscribed to by a superuser, the creator of the topic, or a user who has been explicitly granted subscribe permissions.

The specific SQL syntax is as follows:

```sql
GRANT SUBSCRIBE ON topic_name TO user_name

REVOKE SUBSCRIBE ON topic_name FROM user_name
```

## Tag-Based Authorization (Table-Level Authorization)

Starting from TDengine 3.0.5.0, we support granting permissions to specific subtables within a supertable based on tags. The specific SQL syntax is as follows.

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

- Users can grant or revoke read and write permissions for specified tables (including supertables and regular tables) through dbname.tbname, but cannot directly grant or revoke permissions for subtables.
- Users can grant or revoke read and write permissions for all subtables that meet the conditions through dbname.tbname and the WITH clause. When using the WITH clause, the permission level must be for a supertable.

## Relationship Between Table-Level Permissions and Database Permissions

The table below lists the actual permissions produced under different combinations of database authorization and table-level authorization.

|                  | **No Table Authorization** | **Table Read Authorization**                             | **Table Read Authorization with Tag Conditions**                                       | **Table Write Authorization**                             | **Table Write Authorization with Tag Conditions**                                     |
| ---------------- | -------------------------- | -------------------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------- | ---------------------------------------------------------- |
| **No Database Authorization** | No Authorization           | Read permission for this table, no permissions for other tables in the database | Read permission for subtables of this table that meet tag conditions, no permissions for other tables in the database   | Write permission for this table, no permissions for other tables in the database | Write permission for subtables of this table that meet tag conditions, no permissions for other tables in the database |
| **Database Read Authorization** | Read permission for all tables | Read permission for all tables                         | Read permission for subtables of this table that meet tag conditions, read permissions for other tables in the database | Write permission for this table, read permissions for all tables         | Write permission for subtables of this table that meet tag conditions, read permissions for all tables           |
| **Database Write Authorization** | Write permission for all tables | Read permission for this table, write permissions for all tables         | Read permission for subtables of this table that meet tag conditions, write permissions for all tables           | Write permission for all tables                         | Write permission for subtables of this table that meet tag conditions, write permissions for other tables in the database |

## View User Authorization

Use the following command to display the authorizations a user has:

```sql
show user privileges 
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

1. Revoke data subscription authorization

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

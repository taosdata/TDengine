---
toc_max_heading_level: 4
title: "View"
sidebar_label: "View"
---

## Introduction

Starting from TDengine 3.2.1.0, TDengine Enterprise Edition provides view function, which is convenient for users to simplify operation and improve sharing ability among users.

A view is essentially a query statement stored in a database. The view (non-materialized view) itself does not contain data, and only dynamically executes the query statement specified by the view when reading data from the view. We specify a name when creating a view, and then we can query it like using a regular table. The use of views should follow the following rules:
- Views can be defined and used nested, and are bound to the specified or current database when created.
- Within the same database, duplicate view names are not allowed, and it is recommended not to have duplicate view names and table names (not mandatory). When the view and table names have the same name, operations such as writing, querying, authorizing, and revoking permissions will prioritize using the same-named table.



## Grammar

### Create (update) view

```sql
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

Description:
- When creating a view, you can specify the database name ( db_name ) to which the view is bound. If not explicitly specified, it defaults to the database bound to the current connection.
- It is recommended to specify the database name in the query statement, support cross-database views, and default to the database bound to the view when not specified (it may not be the database specified by the current connection);

### View View

1. View all views under a database

```sql
SHOW [db_name.]VIEWS;
```

2. View the creation statement of the view

```sql
SHOW CREATE VIEW [db_name.]view_name;
```

3. View view column information

```sql
DESCRIBE [db_name.]view_name;
```

4. View all view information

```sql
SELECT ... FROM information_schema.ins_views;
```

### Delete a view

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

## Permissions

### Description
View permissions are divided into three types: READ, WRITE, and ALTER. Query operations require READ permissions, write operations require WRITE permissions, and delete and modify operations on the view itself require ALTER permissions.

### Rules
- The creator of the view and the root user have all permissions by default.
- Authorization and revocation of permissions for other users can be performed through the GRANT and REVOKE statements, which can only be performed by the root user.
- View permissions need to be authorized and revoked separately. Authorization and revocation through db. * do not include view permissions.
- Views can be defined and used nested, and the verification of view permissions is also performed by recursion.
- In order to facilitate the sharing and use of views, the concept of view effective user (i.e. the user who creates the view) is introduced. Authorized users can use the read and write permissions of the view effective user's library, table, and nested view. Note: After the view is REPLACE, the effective user will also be updated.

The detailed rules for controlling relevant permissions are summarized as follows:

| Serial number | Operation | Permission requirements |
| --- | --- | --- |
| 1 | CREATE OR REPLACE VIEW (Create a new view) | The user has WRITE permission on the database to which the view belongs And Users have query permissions for the target library, table, and view of the view. If the object in the query is a view, it must meet rule 8 in the current table. |
| 2 | CREATE OR REPLACE VIEW (Overwrite old view) | The user has WRITE permission on the database to which the view belongs, and ALTER permission on the old view And Users have query permissions for the target library, table, and view of the view. If the object in the query is a view, it must meet rule 8 in the current table. |
| 3 | DROP VIEW | The user has ALTER permission on the view |
| 4 | SHOW VIEWS | No |
| 5 | SHOW CREATE VIEW | No |
| 6 | DESCRIBE VIEW | No |
| 7 | System table query | No |
| 8 | SELECT FROM VIEW | The operating user has READ permissions for the view And Operating users or view effective users have READ permissions on the target library, table, and view of the view |
| 9 | INSERT INTO VIEW | The operation user has WRITE permission on the view And Operating users or view effective users have WRITE permissions on the target library, table, and view of the view |
| 10 | GRANT/REVOKE | Only the root user has permission |


### Grammar

#### Authorization

```sql
GRANT privileges ON [db_name.]view_name TO user_name
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
  | ALTER
}
```

#### Recover permissions

```sql
REVOKE privileges ON [db_name.]view_name FROM user_name
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
  | ALTER
}
```

## Usage scenarios

| SQL query | SQL write | STMT query | STMT write | Subscribe | Stream |
| --- | --- | --- | --- | --- | --- |
| Support | Not supported yet | Not supported yet | Not supported yet | Support | Not supported yet |



## Example

- Create a view

```sql
CREATE VIEW view1 AS SELECT _wstart, count(*) FROM table1 INTERVAL(1d);
CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
CREATE VIEW view3 AS SELECT * from view1;
```

- Query data

```sql
SELECT * from view1;
```

- Delete data

```sql
DROP VIEW view1;
```

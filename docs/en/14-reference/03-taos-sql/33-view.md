---
title: Views
slug: /tdengine-reference/sql-manual/manage-views
---

Starting from TDengine 3.2.1.0, TDengine Enterprise Edition provides the functionality of views, which simplifies operations and enhances sharing capabilities among users.

A view (View) is essentially a query statement stored in the database. Views (non-materialized views) do not contain data themselves; the specified query statement is dynamically executed only when data is read from the view. When creating a view, we specify a name for it, and then it can be queried and operated on like a regular table. The use of views must follow these rules:

- Views can be nested in definitions and usage, and are bound to the database specified at creation time or the current database.
- Within the same database, view names must not be duplicated, and it is recommended that view names do not duplicate table names (not enforced). When a view and a table have the same name, operations such as writing, querying, granting, and revoking permissions prioritize the table with the same name.

## Syntax

### Creating (Updating) a View

```sql
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

Explanation:

- When creating a view, you can specify the database name (db_name) to which the view is bound. If not specified, it defaults to the database bound to the current connection;
- It is recommended to specify the database name in the query statement (query), supporting cross-database views. If not specified, it defaults to the database bound to the view (which may not be the database specified by the current connection);

### Viewing Views

1. View all views under a certain database

  ```sql
  SHOW [db_name.]VIEWS;
  ```

1. View the creation statement of a view

  ```sql
  SHOW CREATE VIEW [db_name.]view_name;
  ```

1. View column information of a view

  ```sql
  DESCRIBE [db_name.]view_name;
  ```

1. View all views information

  ```sql
  SELECT ... FROM information_schema.ins_views;
  ```

### Deleting a View

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

## Permissions

### Explanation

View permissions are divided into READ, WRITE, and ALTER. Query operations require READ permission, write operations require WRITE permission, and modification or deletion of the view itself requires ALTER permission.

### Rules

- The creator of the view and root users have all permissions by default.
- Granting and revoking permissions to other users can be done through the GRANT and REVOKE statements, which can only be performed by root users.
- View permissions must be granted and revoked separately; granting and revoking through db.* does not include view permissions.
- Views can be nested in definitions and usage, and similarly, the verification of view permissions is done recursively.
- To facilitate the sharing and use of views, the concept of an effective user of the view (i.e., the creator of the view) is introduced. Authorized users can use the effective user's database, tables, and nested views' read and write permissions. Note: The effective user is also updated when the view is REPLACEd.

The specific rules for permission control are shown in the table below:

| No. | Operation                                    | Permission Requirements                                                                                                                                                    |
| ---- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | CREATE VIEW <br/>(create new view)            | User has WRITE permission on the database to which the view belongs<br/>and<br/> User has query permissions on the target database, table, or view of the view, if the object in the query is a view, it must satisfy rule 8 in this table                             |
| 2    | CREATE OR REPLACE VIEW <br/>(overwrite old view) | User has WRITE permission on the database to which the view belongs and has ALTER permission on the old view <br/>and<br/> User has query permissions on the target database, table, or view of the view, if the object in the query is a view, it must satisfy rule 8 in this table |
| 3    | DROP VIEW                               | User has ALTER permission on the view                                                                                                                                     |
| 4    | SHOW VIEWS                              | None                                                                                                                                                          |
| 5    | SHOW CREATE VIEW                        | None                                                                                                                                                          |
| 6    | DESCRIBE VIEW                           | None                                                                                                                                                          |
| 7    | System table query                              | None                                                                                                                                                          |
| 8    | SELECT FROM VIEW                        | Operating user has READ permission on the view and the operating user or the effective user of the view has READ permission on the target database, table, or view                                                                   |
| 9    | INSERT INTO VIEW                        | Operating user has WRITE permission on the view and the operating user or the effective user of the view has WRITE permission on the target database, table, or view                                                                 |
| 10   | GRANT/REVOKE                            | Only root users have permission                                                                                                                                        |

### Syntax

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

#### Revoke Permissions

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

## Use Cases

| SQL Query | SQL Write | STMT Query | STMT Write | Subscription | Stream Computing |
| --------- | --------- | ---------- | ---------- | ------------ | ---------------- |
| Supported | Not supported | Not supported | Not supported | Supported | Not supported |

## Examples

- Create View
  
  ```sql
  CREATE VIEW view1 AS SELECT _wstart, count(*) FROM table1 INTERVAL(1d);
  CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
  CREATE VIEW view3 AS SELECT * from view1;
  ```

- Query Data
  
  ```sql
  SELECT * from view1;
  ```

- Delete View
  
  ```sql
  DROP VIEW view1;
  ```

---
title: Manage Views
slug: /tdengine-reference/sql-manual/manage-views
---

Starting from TDengine version 3.2.1.0, TDengine Enterprise Edition provides view functionality to simplify operations and enhance sharing capabilities among users.

A view is essentially a stored query statement in the database. A view (non-materialized view) does not contain data by itself; it dynamically executes the specified query statement only when data is read from the view. When creating a view, we specify a name, and it can be queried just like a table. The following rules apply to the use of views:

- Views can be nested and are bound to the specified or current database at creation time.
- Within the same database, view names cannot be duplicated, and it is recommended that view names are not the same as table names (though it is not mandatory). If a view and a table share the same name, operations such as writing, querying, granting, and revoking permissions will prioritize the table with the same name.

## Syntax

### Create (Update) a View

```sql
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

**Explanation:**

- When creating a view, you can specify the database name (`db_name`) to which the view is bound; if not specified, the current connection's database is used by default.
- In the query statement (`query`), it is recommended to specify the database name. Cross-database views are supported, and if not specified, it defaults to the database bound to the view (which may not be the current connection's database).

### View a View

1. View all views under a specific database:

   ```sql
   SHOW [db_name.]VIEWS;
   ```

2. View the creation statement of a view:

   ```sql
   SHOW CREATE VIEW [db_name.]view_name;
   ```

3. View column information of a view:

   ```sql
   DESCRIBE [db_name.]view_name;
   ```

4. View all view information:

   ```sql
   SELECT ... FROM information_schema.ins_views;
   ```

### Drop a View

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

## Permissions

### Explanation

Permissions for views are divided into three types: READ, WRITE, and ALTER. A query operation requires READ permission, a write operation requires WRITE permission, and delete or modify operations on the view itself require ALTER permission.

### Rules

- The creator of the view and the root user have all permissions by default.
- Permissions can be granted and revoked to other users through the GRANT and REVOKE statements, which can only be performed by the root user.
- View permissions must be granted and revoked separately; authorizations using `db.*` do not include view permissions.
- Views can be defined and used in a nested manner, and permission checks for views are performed recursively.
- To facilitate sharing and usage of views, the concept of an effective user for the view (i.e., the user who created the view) is introduced. Authorized users can use the effective user's permissions on the database, tables, and nested views. Note: The effective user will also be updated if the view is REPLACED.

The specific details of related permission controls are shown in the following table:

| No. | Operation                                  | Permission Requirements                                                                                                                                                            |
| ---- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | CREATE VIEW <br/>(Create a new view)     | User must have WRITE permission on the database to which the view belongs <br/> and <br/> User must have query permissions on the target database, tables, and views of the view; if the queried object is a view, it must meet rule 8 in the current table.                             |
| 2    | CREATE OR REPLACE VIEW <br/>(Overwrite an old view) | User must have WRITE permission on the database to which the view belongs and ALTER permission on the old view <br/> and <br/> User must have query permissions on the target database, tables, and views of the view; if the queried object is a view, it must meet rule 8 in the current table. |
| 3    | DROP VIEW                                 | User must have ALTER permission on the view                                                                                                                                       |
| 4    | SHOW VIEWS                                | None                                                                                                                                                                               |
| 5    | SHOW CREATE VIEW                          | None                                                                                                                                                                               |
| 6    | DESCRIBE VIEW                             | None                                                                                                                                                                               |
| 7    | System Table Queries                      | None                                                                                                                                                                               |
| 8    | SELECT FROM VIEW                          | The operation user must have READ permission on the view and either the operation user or the effective user of the view must have READ permission on the target database, tables, and views of the view.   |
| 9    | INSERT INTO VIEW                          | The operation user must have WRITE permission on the view and either the operation user or the effective user of the view must have WRITE permission on the target database, tables, and views of the view.   |
| 10   | GRANT/REVOKE                              | Only the root user has permission                                                                                                                                                  |

### Syntax

#### Granting Permissions

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

#### Revoking Permissions

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

| SQL Query | SQL Write | STMT Query | STMT Write | Subscription | Stream Calculation |
| --------- | --------- | ---------- | ---------- | ------------ | ------------------ |
| Supported | Not supported | Not supported | Not supported | Supported | Not supported |

## Examples

- Create a View
  
  ```sql
  CREATE VIEW view1 AS SELECT _wstart, count(*) FROM table1 INTERVAL(1d);
  CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
  CREATE VIEW view3 AS SELECT * from view1;
  ```

- Query Data
  
  ```sql
  SELECT * from view1;
  ```

- Drop a View
  
  ```sql
  DROP VIEW view1;
  ```

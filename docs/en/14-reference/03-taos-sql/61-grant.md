---
title: Permissions
slug: /tdengine-reference/sql-manual/manage-permissions
---

In TDengine, permission management is divided into [user management](../manage-users/), database authorization management, and message subscription authorization management. This section focuses on database authorization and subscription authorization. The authorization function only available in TDengine Enterprise Edition. Although authorization syntax is available in the community version 3.3.x.y and earlier, but has no effect. In 3.4.0.0 and later community versions, the authorization syntax will report an error directly.

Starting from 3.4.0.0, TDengine Enterprise Edition implements a separation of three powers mechanism through role-based access control (RBAC), with significant changes to permissions. Some syntax is no longer compatible. The subsequent sections of this document will explain the differences.

## Version Comparison

| Feature | 3.3.x.y- | 3.4.0.0+ |
|---------|---------|----------|
| Basic User Management | ✓ | ✓ |
| RBAC Role Management | ✗ | ✓ |
| Separation of Three Powers (SYSDBA/SYSSEC/SYSAUDIT) | ✗ | ✓ |
| Fine-grained Permissions | ✗ | ✓ |
| Audit Database Permissions | ✗ | ✓ |
| Table Permissions | ✓ | ✓ |
| Row Permissions | ✗ | ✓ |
| Column Permissions | ✗ | ✓ |

---

## Permission Management - 3.3.x.y and Earlier Versions

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

2. Revoke data subscription authorization

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

---

## Permission Management - 3.4.0.0 and Later Versions

### Overview of Separation of Three Powers

Starting from 3.4.0.0, TDengine Enterprise Edition implements a separation of three powers mechanism through role-based access control (RBAC). The management permissions of the root user are split into SYSDBA, SYSSEC, and SYSAUDIT three system management permissions, achieving effective isolation and balance of powers.

| Role | Full Name | Responsibilities |
|------|-----------|------------------|
| **SYSDBA** | Database Administrator | Database daily maintenance, system management, user and role creation. Cannot perform operations related to SYSSEC/SYSAUDIT |
| **SYSSEC** | Database Security Administrator | User and role permission grant/revoke, security policy formulation |
| **SYSAUDIT** | Database Audit Administrator | Independent audit supervision, audit database management, audit log viewing. Cannot view business data |

**Key Constraints:**

```text
❌ Not allowed to grant both SYSDBA/SYSSEC/SYSAUDIT to the same user at the same time
✓ System allows multiple users to own the same system role
✓ System management role permissions cannot be changed through the command line, support automatic updates during upgrades
```

### Root User and System Roles

**Initial State:** Root user has full permissions for SYSDBA, SYSSEC, and SYSAUDIT by default

**Recommended Practice:** After initial system configuration, immediately separate roles, then disable root for daily operations

```sql
-- Create dedicated administrators
CREATE USER dba_user PASS 'DbaPass123!@#';
CREATE USER sec_user PASS 'SecPass123!@#';
CREATE USER audit_user PASS 'AuditPass123!@#';

-- Separate authorization
GRANT ROLE `SYSDBA` TO dba_user;
GRANT ROLE `SYSSEC` TO sec_user;
GRANT ROLE `SYSAUDIT` TO audit_user;
```

### Database Administrator (SYSDBA)

**Responsibilities:**

- Database daily maintenance and system management
- Create and manage users and roles
- Manage database objects such as tables, indexes, etc.
- Manage system resources such as nodes, stream computing, subscriptions, etc.

**Restrictions:**

- Cannot grant SYSSEC/SYSAUDIT permissions
- Cannot perform operations related to audit databases
- By default does not have permission to view business data (but can view metadata)

### Database Security Administrator (SYSSEC)

**Responsibilities:**

- User and role permission management (except SYSDBA/SYSAUDIT)
- Security parameter configuration
- TOTP key management
- User security information settings

**Permission Examples:**

```sql
GRANT/REVOKE SYSSEC PRIVILEGE
ALTER SECURITY VARIABLE
CREATE TOTP / DROP TOTP
SET USER SECURITY INFORMATION
READ INFORMATION_SCHEMA SECURITY
```

### Database Audit Administrator (SYSAUDIT)

**Responsibilities:**

- Independent audit supervision
- Audit database management
- Audit log viewing
- Audit parameter configuration

**Permission Examples:**

```sql
GRANT/REVOKE SYSAUDIT PRIVILEGE
ALTER/DROP/USE AUDIT DATABASE
SELECT AUDIT TABLE
SET USER AUDIT INFORMATION
READ INFORMATION_SCHEMA AUDIT
```

### Role Management

#### Creating Roles

```sql
CREATE ROLE [IF NOT EXISTS] role_name;
```

**Constraints:**

- Creator needs CREATE ROLE permission
- Role name length 1-63 characters
- Role name cannot be the same as an existing user name

#### Deleting and Viewing Roles

```sql
-- Delete role
DROP ROLE [IF EXISTS] role_name;

-- View role list
SHOW ROLES;
SELECT * FROM information_schema.ins_roles;

-- View role permissions
SHOW ROLE PRIVILEGES;
SELECT * FROM information_schema.ins_role_privileges;
```

#### Role Disable/Enable

```sql
LOCK ROLE role_name;
UNLOCK ROLE role_name;
```

#### Role Grant and Revoke

```sql
GRANT ROLE role_name TO user_name;
REVOKE ROLE role_name FROM user_name;
```

### System Built-in Roles

In addition to the three major system management roles, TDengine provides the following system built-in roles:

| Role | Description |
|------|-------------|
| **SYSAUDIT_LOG** | Can create tables and write data in audit database, but cannot delete/modify tables/delete data. Cannot be granted to a user at the same time as SYSDBA/SYSSEC/SYSAUDIT |
| **SYSINFO_0** | Corresponds to SYSINFO=0 permission, view basic system information |
| **SYSINFO_1** | Corresponds to SYSINFO=1 permission, view more system information, can modify own password |

### System Permission Management

3.4.0.0+ introduces fine-grained system permissions:

```sql
-- Grant system permissions
GRANT privileges TO {user_name | role_name};
-- Revoke system permissions
REVOKE privileges FROM {user_name | role_name};

privileges: {
  priv_type [, priv_type] ...
}

priv_type: {
    -- Database permissions
    CREATE DATABASE

    -- Function permissions
  | CREATE FUNCTION | DROP FUNCTION | SHOW FUNCTIONS

    -- Mount permissions
  | CREATE MOUNT | DROP MOUNT | SHOW MOUNTS

    -- User permissions
  | CREATE USER | DROP USER | ALTER USER
  | SET USER BASIC INFORMATION | SET USER SECURITY INFORMATION | SET USER AUDIT INFORMATION
  | UNLOCK USER | LOCK USER | SHOW USERS

    -- Token permissions
  | CREATE TOKEN | DROP TOKEN | ALTER TOKEN | SHOW TOKENS

    -- Role permissions
  | CREATE ROLE | DROP ROLE | SHOW ROLES | LOCK ROLE | UNLOCK ROLE

    -- Key permissions
  | CREATE TOTP | DROP TOTP
  
    -- Password permissions
  | ALTER PASS | ALTER SELF PASS

    -- Node permissions
  | CREATE NODE | DROP NODE | SHOW NODES

    -- Permission grant/revoke permissions
  ｜GRANT PRIVILEGE ｜ REVOKE PRIVILEGE | SHOW PRIVILEGES

    -- System variable permissions
  | ALTER SECURITY VARIABLE | ALTER AUDIT VARIABLE   | ALTER SYSTEM VARIABLE | ALTER DEBUG VARIABLE
  | SHOW SECURITY VARIABLES | SHOW AUDIT VARIABLES | SHOW SYSTEM VARIABLES | SHOW DEBUG VARIABLES

    -- System management permissions
  | READ INFORMATION_SCHEMA BASIC | READ INFORMATION_SCHEMA PRIVILEGED
  | READ INFORMATION_SCHEMA SECURITY | READ INFORMATION_SCHEMA AUDIT 
  | READ PERFORMANCE_SCHEMA BASIC | READ PERFORMANCE_SCHEMA PRIVILEGED
  | SHOW TRANSACTIONS | KILL TRANSACTION
  | SHOW CONNECTIONS | KILL CONNECTION
  | SHOW QUERIES | KILL QUERY
  | SHOW GRANTS | SHOW CLUSTER | SHOW APPS

}
```

### Object Permission Management

3.4.0.0+ supports more fine-grained object permissions:

```sql
-- Grant object permissions
GRANT privileges ON [priv_obj] priv_level [WITH condition] TO {user_name | role_name}

-- Revoke object permissions
REVOKE privileges ON [priv_obj] priv_level [WITH condition] FROM {user_name | role_name}

-- Permission target objects (defaults to table if not specified)
priv_obj: {
    database           -- Database
  | table              -- Table
  | view               -- View
  | index              -- Index
  | tsma               -- Window pre-aggregation
  | rsma               -- Downsampling storage
  | topic              -- Topic
  | stream             -- Stream computing
}

priv_level: {
    *                  -- All databases
  | dbname             -- Specified database
  | *.*                -- All databases, all objects
  | dbname.*           -- Specified database, all objects
  | dbname.objname     -- Specified database, specified object
}

privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}

column_list: {
    columnName [,columnName] ...
}

priv_type: {
    ALTER | DROP
  | SELECT [(column_list)] | INSERT [(column_list)] | DELETE
  | CREATE TABLE | CREATE VIEW | CREATE INDEX | CREATE TSMA | CREATE RSMA | CREATE TOPIC | CREATE STREAM
  | USE | SHOW | SHOW CREATE
  | FLUSH | COMPACT | TRIM | ROLLUP | SCAN | SSMIGRATE
  | SUBSCRIBE | SHOW CONSUMERS | SHOW SUBSCRIPTIONS
  | START | STOP | RECALCULATE
}
```

#### Object Type and Permission Type Mapping

Different object types support different permission types. The specific mapping is as follows:

| Permission Type | database | table | view | index | tsma | rsma | topic | stream |
|---------|:--------:|:-----:|:----:|:-----:|:----:|:----:|:-----:|:------:|
| ALTER | ✓ | ✓ | ✓ | | | ✓ | | |
| DROP | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| SELECT [(column_list)] | | ✓ | ✓ | | | | | |
| INSERT [(column_list)] | | ✓ | | | | | | |
| DELETE | | ✓ | | | | | | |
| CREATE TABLE | ✓ | | | | | | | |
| CREATE VIEW | ✓ | | | | | | | |
| CREATE INDEX | | ✓ | | | | | | |
| CREATE TSMA | | ✓ | | | | | | |
| CREATE RSMA | | ✓ | | | | | | |
| CREATE TOPIC | ✓ | | | | | | | |
| CREATE STREAM | ✓ | | | | | | | |
| USE | ✓ | | | | | | | |
| SHOW | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| SHOW CREATE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| FLUSH | ✓ | | | | | | | |
| COMPACT | ✓ | | | | | | | |
| TRIM | ✓ | | | | | | | |
| ROLLUP | ✓ | | | | | | | |
| SCAN | ✓ | | | | | | | |
| SSMIGRATE | ✓ | | | | | | | |
| SUBSCRIBE | | | | | | | ✓ | |
| SHOW CONSUMERS | | | | | | | ✓ | |
| SHOW SUBSCRIPTIONS | | | | | | | ✓ | |
| START | | | | | | | | ✓ |
| STOP | | | | | | | | ✓ |
| RECALCULATE | | | | | | | | ✓ |

**Notes:**

- When using `GRANT` for authorization, you need to specify the object type through `ON [priv_obj]`, and the system will automatically verify whether the permission is applicable to the specified object type.
- `[(column_list)]` indicates an optional list of column names for implementing column-level permission control. For `view` object, only `SELECT` is supported, and column-list is not supported.
- Only one rule can be set for the same type of operation per table for column permissions.

#### Database Permissions

Database permissions are used to control user access and operations on databases. Database permissions can be applied at different levels.

**Permission Application Levels:**

- `*`: All databases
- `dbname`: Specified database

**Common Permission Combinations:**

| Permission Combination | Description | Use Case |
|---------|------|---------|
| USE | Use (access) database | Basic database access |
| ALTER | Modify database parameters | Database configuration adjustment |
| DROP | Delete database | Database cleanup, uninstall |
| CREATE TABLE | Create table | Table structure creation |
| CREATE VIEW | Create view | View creation |
| CREATE TOPIC | Create topic | Topic creation |
| CREATE STREAM | Create stream | Stream computing creation |
| SHOW | View database information | List objects in database |
| FLUSH | Flush database | Force data persistence |
| COMPACT | Compact database | Database maintenance, optimization |

**Example - Database Permission Grant:**

```sql
-- User developer can use power database
GRANT USE ON DATABASE power TO developer;

-- User can access all databases
GRANT USE ON DATABASE * TO analyst;

-- User can create tables and views in power database
GRANT CREATE TABLE, CREATE VIEW ON DATABASE power TO creator;

-- User can modify database configuration
GRANT ALTER ON DATABASE power TO dba_user;

-- User can view database `power` by `show databases` command
GRANT SHOW ON DATABASE power TO viewer;

-- User has full management permissions on database
GRANT ALL ON DATABASE power TO admin_user;

-- Revoke user's database permissions
REVOKE ALL ON DATABASE power FROM developer;
```

**Database Permission Special Notes:**

- **Owner Concept**: Database creator has full permissions on the database by default

#### Table Permissions

Table permissions are used to control user access and operations on tables. Table permissions can be applied at different levels:

**Permission Application Levels:**

- `*.*`: All tables in all databases
- `dbname.*`: All tables in specified database
- `dbname.tbname`: Specified table in specified database

**Common Permission Combinations:**

| Permission Combination | Description | Use Case |
|---------|------|---------|
| SELECT | Query table data | Data analysis, report queries |
| INSERT | Write table data | Data collection, real-time writes |
| SELECT, INSERT | Read and write data | Data processing, ETL operations |
| SELECT, INSERT, DELETE | Complete operations | Data maintenance, data cleanup |
| ALTER, DROP | Modify table structure | Table structure management, maintenance |

**Example - Table Permission Grant:**

```sql
-- User can only query power.meters table
GRANT SELECT ON power.meters TO analyst;

-- User can write data to all tables in power database
GRANT INSERT ON power.* TO collector;

-- User can SELECT, INSERT, DELETE on power.devices table
GRANT SELECT, INSERT, DELETE ON power.devices TO operator;

-- User has full operation permissions on power.devices table
GRANT ALL ON power.devices to operator;

-- User can modify table structure in power database
GRANT ALTER ON power.* TO dba_user;

```

**Table Permission Policy and Priority:**

- Subtable permissions > supertable permissions. If no subtable permissions, subtables inherit supertable permissions.
- Explicitly specified table name permissions > implied wildcard * permissions.
- Table owner has complete operation permissions on the table; subtables inherit the supertable owner.

#### Row Permissions

Row permissions are used to restrict users to only access rows in tables that meet specific conditions. Specify row filter conditions using the `WITH` clause.

**Syntax:**

```sql
GRANT SELECT ON table_name WITH condition TO user_name;
REVOKE SELECT ON table_name FROM user_name; // the revoke will take effect regardless of whether a condition is specified
REVOKE ALL ON table_name FROM user_name;
```

**Condition Rules:**

- Conditions apply to supertables or regular tables
- Cannot specify conditions for subtables
- Only one rule can be set for the same type of operation per table
- Multiple conditions can use `AND/OR` combination
- Can be combined with tag conditions
- Can be combined with column permissions

**Example - Row Permission by Data Source:**

```sql
-- User u1 can only see data from sensor_001
GRANT SELECT ON power.meters WITH source='sensor_001' TO u1;

-- User u2 can only see data with temperature > 30°C
GRANT SELECT ON power.meters WITH temperature > 30 TO u2;

-- User u3 can view/write/delete data within time range
GRANT SELECT, INSERT, DELETE ON power.meters WITH ts >= '2024-01-01' AND ts < '2024-02-01' TO u3;
```

#### Column Permissions

Column permissions are used to restrict users to only access specific columns in tables. Only supported in `SELECT` or `INSERT` permissions.

**Syntax:**

```sql
GRANT SELECT (col1, col2, ...) ON table_name TO user_name;
GRANT INSERT (col1, col2, ...) ON table_name TO user_name;
REVOKE SELECT,INSERT ON table_name FROM user_name;
REVOKE ALL ON table_name FROM user_name;
```

**Column Permission Rules:**

- Only applies to `SELECT` and `INSERT` operations
- Can only specify supertables or regular tables, not subtables
- Only one rule per table per operation
- Can be used together with row permissions

**Example - Permission by Column:**

```sql
-- User analyst can only see ts and power columns
GRANT SELECT (ts, power) ON power.meters TO analyst;

-- User writer can only write to temperature column
GRANT INSERT (ts, temperature) ON power.meters TO writer;

-- User limited_user can only see device_id and status columns
GRANT SELECT (device_id, status) ON power.meters TO limited_user;
```

**Example - Combining Row and Column Permissions:**

```sql
-- User can only see power and status columns within time range
GRANT SELECT (ts, power, status) ON power.meters WITH ts >= '2024-01-01' TO analyst;

-- User can view/write/delete temperature data from specific sensor
GRANT SELECT, INSERT (ts, temperature), DELETE ON power.meters WITH source='sensor_001' TO collector;
```

**Row/Column Permission Priority:**

- Later update rules take effect
- Same update time, user permissions take precedence over role permissions
- Union of different types of permissions between users and roles

### View Permissions

View permissions are used to control user access and operations on views. View permissions need to be granted separately; database permissions do not include view permissions.

**Common Permission Combinations:**

| Permission | Description | Use Case |
|------|------|---------|
| SELECT [VIEW] | Query view data | Data analysis, report queries |
| DROP [VIEW] | Delete view | View cleanup and maintenance |
| ALTER [VIEW] | Modify view definition | View structure adjustment |
| SHOW [VIEWS] | View list of views | See views in the system |
| SHOW CREATE [VIEW] | View view definition | See view creation statement |

**Example - View Permission Grant:**

```sql
-- User analyst can query view meter_stats in power database
GRANT SELECT ON VIEW power.meter_stats TO analyst;

-- User can modify view definition
GRANT ALTER ON VIEW power.meter_stats TO maintainer;

-- User can view list and definition of views
GRANT SHOW, SHOW CREATE ON VIEW power.meter_stats TO viewer;

-- User has full operation permissions on view
GRANT ALL ON VIEW power.meter_stats TO admin_user;

-- Revoke all permissions on view from user
REVOKE ALL ON VIEW power.meter_stats FROM analyst;
```

**View Permission Special Notes:**

- **Creation Permission**: View creation permission is controlled through `CREATE VIEW` database permission
- **Owner Permissions**: View creator has full permissions on the view by default; can use nested views (view effective user concept)
- **Nested Views**: Authorized users can use the effective user's databases, tables, and nested views' read and write permissions
- **Permission Inheritance**: View permissions need to be granted separately; grants through `dbname.*` do not include view permissions

**Permission Priority:**

- Explicitly specified view name permissions > wildcard permissions

### Topic Permissions

Topic permissions are used to control user access and operations on message topics. TDengine supports fine-grained permission control on topics.

**Permission Application Levels:**

- `*.*`: All topics
- `dbname.topicname`: Specified topic in specified database

**Common Permission Combinations:**

| Permission | Description | Use Case |
|------|------|---------|
| SUBSCRIBE | Subscribe to topic | Data consumer subscribe to message stream |
| SHOW TOPICS | View topic list | See topics in the system |
| SHOW CREATE TOPIC | View topic definition | See topic creation statement |
| DROP TOPIC | Delete topic | Topic cleanup and maintenance |
| SHOW CONSUMERS | View consumers | Monitor subscription status |
| SHOW SUBSCRIPTIONS | View subscription information | Understand subscription relationships |

**Example - Topic Permission Grant:**

```sql
-- User consumer1 can subscribe to device_events topic in power database
GRANT SUBSCRIBE ON power.device_events TO consumer1;

-- User consumer2 can subscribe to all topics in all databases
GRANT SUBSCRIBE ON *.* TO consumer2;

-- User can view all topics of database `power`
GRANT SHOW ON TOPIC power.* TO viewer;

-- User can view topic definition and consumer information
GRANT SHOW CREATE, SHOW CONSUMERS ON TOPIC power.device_events TO inspector;

-- User has full management permissions on topic
GRANT ALL ON TOPIC power.device_events TO admin_user;

-- Revoke all topic permissions from inspector
REVOKE ALL ON TOPIC power.device_events FROM inspector;
```

**Topic Permission Special Notes:**

- **Creation Permission**: Topic creation permission is controlled through `CREATE TOPIC` database permission; any user with `CREATE TOPIC` permission can create topics on that database
- **Delete Permission**: Only topic creator and users with `DROP TOPIC` permission can delete topics
- **Consumer Management**: Users with `SHOW CONSUMERS` permission can view consumer information of that topic

**Permission Priority:**

- Explicitly specified topic name permissions > wildcard `*` permissions

### Stream Computing Permissions

Stream computing permissions are used to control user access and operations on streams.

**Permission Application Levels:**

- `*.*`: All streams
- `dbname.*`: All streams in specified database
- `dbname.stream_name`: Specified stream in specified database

**Common Permission Combinations:**

| Permission | Description | Use Case |
|------|------|---------|
| SHOW [STREAMS] | View stream list | See streams in the system |
| SHOW CREATE [STREAM] | View stream definition | See stream creation statement and configuration |
| DROP [STREAM] | Delete stream | Stream cleanup and maintenance |
| START [STREAM] | Start stream | Start data processing |
| STOP [STREAM] | Stop stream | Pause data processing |
| RECALCULATE [STREAM] | Recalculate stream | Reprocess stream data |

**Example - Stream Permission Grant:**

```sql
-- User processor can view all streams in power database
GRANT SHOW ON STREAM power.* TO processor;

-- User can start and stop specific stream
GRANT START, STOP ON STREAM power.realtime_agg TO operator;

-- User can view stream definition and operate stream
GRANT SHOW CREATE, START, STOP ON STREAM power.* TO manager;

-- User has full management permissions on stream
GRANT ALL ON STREAM power.realtime_agg TO admin_user;

-- Revoke user's start permission
REVOKE START ON STREAM power.realtime_agg FROM operator;

-- Revoke all permissions on stream from user
REVOKE ALL ON STREAM power.realtime_agg FROM operator;
```

### Audit Database

3.4.0.0+ specifically supports audit databases:

**Features:**

- System allows only one audit database
- Audit database is identified by `is_audit` attribute (not a fixed name)
- Only SYSAUDIT can delete and modify audit databases
- To prevent accidental deletion, a new `allow_drop` attribute has been added. Audit database defaults to 0, regular databases default to 1. To delete an audit database, you need to set `allow_drop` to 1.

**Permission Restrictions:**

```text
❌ No one is allowed to delete audit tables
❌ No one is allowed to modify audit tables
❌ No one is allowed to delete data from audit tables
✓ Only SYSAUDIT_LOG role can write data to audit database
✓ Only SYSAUDIT role can view data in audit database tables
```

### Owner (Owner) Concept

3.4.0.0+ clarifies the permissions of object owners:

- **Owner**: Creator of database objects or recipient of transferred ownership
- **Implicit Permissions**: Owner has full permissions on the object without needing authorization
- **Management Permissions**: Can modify object structure, delete objects

---

## Permission Viewing

```sql
-- View user permissions (3.4.0.0+)
SHOW USER PRIVILEGES
SELECT * FROM information_schema.ins_user_privileges

taos> show user privileges;
 user_name |    priv_type        |  priv_scope | db_name | table_name | condition |  notes | columns |        update_time         |
===================================================================================================================================
 u1        | CREATE DATABASE     | CLUSTER     |         |            |           |        |         |                            |
 u1        | SUBSCRIBE           | TOPIC       | d0      | topic1     |           |        |         |                            |
 u1        | USE DATABASE        | DATABASE    | d0      |            |           |        |         |                            |
 u1        | CREATE TABLE        | DATABASE    | d0      |            |           |        |         |                            |
 u1        | ALTER               | VIEW        | d0      | v1         |           |        |         |                            |
 u1        | SELECT VIEW         | VIEW        | d0      | v1         |           |        |         |                            |
 u1        | SELECT              | TABLE       | d0      | stb0       |           |        | ts,c0   | 2026-01-28 14:39:56.960258 |
 u1        | INSERT              | TABLE       | d0      | stb0       |           |        | ts,c0   | 2026-01-28 14:39:56.977788 |
 u2        | CREATE DATABASE     | CLUSTER     |         |            |           |        |         |                            |

-- View role permissions (3.4.0.0+)
SHOW ROLE PRIVILEGES
SELECT * FROM information_schema.ins_role_privileges
```

```text
taos> show role privileges;
 role_name      |    priv_type        |  priv_scope | db_name | table_name | condition |  notes | columns |     update_time       |
 ===================================================================================================================================
 SYSSEC         | SHOW CREATE         | TABLE       |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW                | VIEW        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | VIEW        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW                | TSMA        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | TSMA        |  *      |  *         |           |        |         |                       |
 SYSAUDIT_LOG   | USE AUDIT DATABASE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | CREATE AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | INSERT AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
```

---

## Best Practices

### 3.3.x.y- Version

1. Use root to create business users, grant permissions following the principle of least privilege
2. Read-only applications should only be granted READ permission
3. Write applications should only be granted WRITE permission
4. Use tag filters to restrict user access to specific subtables

### 3.4.0.0+ Version

1. **Immediately Separate Three Permissions**: After initialization, assign SYSDBA/SYSSEC/SYSAUDIT to different users
2. **Disable root for Daily Operations**: After configuration completion, no longer use root for daily maintenance
3. **Use Roles to Simplify Permissions**: Create common roles and grant them to users

**Example - Create Read-Only Analysis Role:**

```sql
CREATE ROLE analyst_role;
GRANT SHOW,SELECT ON power.* TO analyst_role;
GRANT SHOW,USE on database power TO analyst_role;
GRANT ROLE analyst_role TO analyst_user;
```

**Example - Create Data Write Role:**

```sql
CREATE ROLE writer_role;
GRANT INSERT ON power.* TO writer_role;
GRANT SHOW,USE,CREATE TABLE ON database power TO writer_role;
GRANT ROLE writer_role TO writer_user;
```

**Example - Security Audit Configuration:**

```sql
-- Create audit database
CREATE DATABASE audit_db KEEP 36500d IS_AUDIT 1 ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2;

-- Create auditor
CREATE USER audit_user PASS 'AuditPass123!@#';
GRANT ROLE `SYSAUDIT` TO audit_user;

-- Create audit log role (for application writes)
CREATE ROLE audit_logger;
GRANT ROLE `SYSAUDIT_LOG` TO audit_logger;
```

---

## Compatibility and Upgrades

| Feature | 3.3.x.y- | 3.4.0.0+ |
|---------|---------|----------|
| CREATE/ALTER/DROP USER | ✓ | ✓ |
| GRANT/REVOKE READ/WRITE | ✓ | ✗ |
| View/Subscription Permissions | ✓ | ✓ |
| Role Management | ✗ | ✓ |
| Separation of Three Powers | ✗ | ✓ |
| Fine-grained Permissions | ✗ | ✓ |
| Audit Database | ✗ | ✓ |

**Upgrade Notes:**

- ✓ Support automatic upgrade after shutdown from lower versions to 3.4.0.0+
- ✗ Rolling upgrade is not supported
- ✗ Cannot downgrade after upgrade

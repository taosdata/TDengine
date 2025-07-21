---
sidebar_label: ODBC
title: ODBC Client Library
slug: /tdengine-reference/client-libraries/odbc
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/odbc-01.png';
import imgStep02 from '../../assets/odbc-02.png';

TDengine ODBC is an ODBC driver implemented for TDengine, supporting applications on Windows systems (such as [PowerBI](https://powerbi.microsoft.com/) and others) as well as user-customized applications to access TDengine databases locally, remotely, and via cloud services through the ODBC standard interface.

TDengine ODBC offers two types of connections to the TDengine database: WebSocket (recommended) and native connections. Different connection methods can be set for the TDengine data source when in use. WebSocket connection must be used when accessing cloud services.

TDengine ODBC provides both 64-bit and 32-bit drivers. However, the 32-bit version is only supported by the TSDB-Enterprise and only supports WebSocket connections.  
**Note:**

- Driver Manager: Ensure to use the ODBC driver manager that matches the architecture of the application. 32-bit applications need a 32-bit ODBC driver manager, and 64-bit applications need a 64-bit ODBC driver manager.
- Data Source Name (DSN): Both 32-bit and 64-bit ODBC driver managers can see all DSNs, and DSNs under the User DSN tab will share the same name, so it is necessary to distinguish them in the DSN name.

## ODBC Version Compatibility

Supports all ODBC versions.

## Installation

1. Only supports the Windows platform. Windows requires the installation of the VC runtime library, which can be downloaded here [VC Runtime Library](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170). If you have already installed the VS development tools, you can ignore this.

2. Install the TDengine Windows client. Version 3.2.1.0 or above includes the TDengine ODBC 64-bit driver; version 3.3.3.0 or above includes the TDengine ODBC 32/64-bit driver.

## Configure Data Source

### Data Source Connection Types and Differences

TDengine ODBC supports two ways to connect to the TDengine database: WebSocket connection and Native connection, with the following differences:

1. Only WebSocket connection mode is supported when accessing cloud services.

2. Only WebSocket connection mode is supported for 32-bit applications.

3. WebSocket connection has better compatibility, generally does not require upgrading the client library with TDengine database server upgrades.

4. Native connection usually performs slightly better, but must be consistent with the version of the TDengine database server.

5. For general users, it is recommended to use the **WebSocket** connection mode, as the performance difference with Native is not significant, and compatibility is better.

### WebSocket Connection

1. Search and open the **ODBC Data Sources (32-bit)** or **ODBC Data Sources (64-bit)** management tool from the **Start** menu

2. Select the **User DSN** tab, and proceed to the "Create Data Source" interface via the **Add(D)** button

3. Choose the data source you want to add, here we select **TDengine**

4. Click finish, enter the TDengine ODBC data source configuration page, fill in the following necessary information

    <figure>
    <Image img={imgStep01} alt=""/>
    </figure>

    4.1 **DSN**: Data Source Name, required, name the newly added ODBC data source

    4.2 **Connection Type**: Required, select the connection type, here choose **WebSocket**

    4.3 **URL**: Required, ODBC data source URL, example: `http://localhost:6041`, cloud service URL example: `https://gw.cloud.tdengine.com?token=your_token`

    4.4 **Database**: Optional, the default database to connect to

    4.5 **Username**: Optional, for use in step 5 connection testing only, database username, if not specified, TDengine defaults to root

    4.6 **Password**: Optional, for use in step 5 connection testing only, database user password, if not specified, TDengine defaults to taosdata

    4.7 **Compatible Software**: Supports compatibility adaptation for industrial software such as KingSCADA, Kepware, etc., usually, the default value General is sufficient

5. Click **Test Connection** to test the connection status. If successful, a "Successfully connected to URL" message will appear.

6. Click **OK** to save the configuration and exit.

7. You can also select a pre-configured data source name in step 2 and enter the configuration page through the **Configure** button to modify existing configurations.

### Native Connection (Does not support cloud services and 32-bit applications)

1. Search and open **ODBC Data Sources (64-bit)** management tool from the **Start** menu (make sure not to select ODBC Data Sources (32-bit)).

2. Select the **User DSN** tab, and enter the "Create Data Source" interface through the **Add(D)** button.

3. Choose the data source you want to add, here we select **TDengine**.

4. Click finish to enter the TDengine ODBC data source configuration page, fill in the necessary information as follows:

    <figure>
    <Image img={imgStep02} alt=""/>
    </figure>

    4.1 **DSN**: Data Source Name is required, name the newly added ODBC data source.

    4.2 **Connection Type**: Mandatory, select the type of connection, here choose **Native**.

    4.3 **Server**: Required, ODBC data source server address, example: `localhost:6030`.

    4.4 **Database**: Optional, the default database to connect to.

    4.5 **Username**: Optional, used only for testing the connection in step 5, database username, if not specified, TDengine defaults to root.

    4.6 **Password**: Optional, used only for testing the connection in step 5, database user password, if not specified, TDengine defaults to taosdata.

    4.7 **Compatible Software**: Supports compatibility adaptation for industrial software such as KingSCADA, Kepware, etc. Usually, the default value General is sufficient.

5. Click **Test Connection** to test the connection status. If successful, a "Connection successful" message will appear.

6. Click **OK** to save the configuration and exit.

7. You can also select a pre-configured data source name in step 2 and enter the configuration page through the **Configure** button to modify existing configurations.

## Supported Platforms

The platforms supported by the native connection method are consistent with those supported by the TDengine Windows X64 client driver.
In addition to this, the WebSocket connection method also supports 32-bit applications running on Windows X64 systems.

## Version History

| taos_odbc Version | Major Changes                                                                                             |   TDengine Version    |
| -----------  | --------------------------------------------------------------------------------------------------  | ----------------  |
|      v1.1.0   | 1. Supports view functionality. <br/>2. Supports VARBINARY/GEOMETRY data types. <br/>3. Supports ODBC 32-bit WebSocket connection method (Enterprise edition only). <br/>4. Supports ODBC data source configuration dialog settings for compatibility adaptation options for industrial software like KingSCADA, Kepware, etc. (Enterprise edition only). | 3.3.3.0 and higher |
|      v1.0.2   | Supports CP1252 character encoding.                                                                                 | 3.2.3.0 and higher |
|      v1.0.1   | 1. Supports DSN settings for BI mode, in BI mode TDengine database does not return system database and supertable subtable information. <br/>2. Refactored character set conversion module, improving read and write performance. <br/> 3. Default connection method in ODBC data source configuration dialog changed to "WebSocket". <br/>4. Added "Test Connection" control in ODBC data source configuration dialog. <br/>5. ODBC data source configuration supports Chinese/English interface. |         -          |
|    v1.0.0.0   | Initial release, supports interacting with TDengine database to read and write data, refer to the "API Reference" section for details.                                | 3.2.2.0 and higher |

## Data Type Mapping

The table below explains how the ODBC connector maps server data types to default SQL and C data types.

| TDengine Type      | SQL Type          | C Type            |
|--------------------|-------------------|-------------------|
| TIMESTAMP          | SQL_TYPE_TIMESTAMP| SQL_C_TIMESTAMP   |
| INT                | SQL_INTEGER       | SQL_C_SLONG       |
| INT UNSIGNED       | SQL_INTEGER       | SQL_C_ULONG       |
| BIGINT             | SQL_BIGINT        | SQL_C_SBIGINT     |
| BIGINT UNSIGNED    | SQL_BIGINT        | SQL_C_UBIGINT     |
| FLOAT              | SQL_REAL          | SQL_C_FLOAT       |
| DOUBLE             | SQL_DOUBLE        | SQL_C_DOUBLE      |
| BINARY             | SQL_BINARY        | SQL_C_BINARY      |
| SMALLINT           | SQL_SMALLINT      | SQL_C_SSHORT      |
| SMALLINT UNSIGNED  | SQL_SMALLINT      | SQL_C_USHORT      |
| TINYINT            | SQL_TINYINT       | SQL_C_STINYINT    |
| TINYINT UNSIGNED   | SQL_TINYINT       | SQL_C_UTINYINT    |
| BOOL               | SQL_BIT           | SQL_C_BIT         |
| NCHAR              | SQL_VARCHAR       | SQL_C_CHAR        |
| VARCHAR            | SQL_VARCHAR       | SQL_C_CHAR        |
| JSON               | SQL_WVARCHAR      | SQL_C_WCHAR       |
| GEOMETRY           | SQL_VARBINARY     | SQL_C_BINARY      |
| VARBINARY          | SQL_VARBINARY     | SQL_C_BINARY      |

## API Reference

### API List

- **Currently exported ODBC functions are**:

| ODBC/Setup API | Linux | macOS | Windows | Note |
| :----- | :---- | :---- | :---- | :---- |
| ConfigDSN | ❌ | ❌ | ✅ | |
| ConfigDriver | ❌ | ❌ | ✅ | |
| ConfigTranslator | ❌ | ❌ | ❌ | |
| SQLAllocHandle | ✅ | ✅ | ✅ | |
| SQLBindCol  | ✅ | ✅ | ✅ | Column-Wise Binding only |
| SQLBindParameter | ✅ | ✅ | ✅ | Column-Wise Binding only |
| SQLBrowseConnect | ❌ | ❌ | ❌ | |
| SQLBulkOperations | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLCloseCursor | ✅ | ✅ | ✅ | |
| SQLColAttribute | ✅ | ✅ | ✅ | |
| SQLColumnPrivileges | ❌ | ❌ | ❌ | TDengine has no strict counterpart |
| SQLColumns | ✅ | ✅ | ✅ | |
| SQLCompleteAsync | ❌ | ❌ | ❌ | |
| SQLConnect | ✅ | ✅ | ✅ | |
| SQLCopyDesc | ❌ | ❌ | ❌ | |
| SQLDescribeCol | ✅ | ✅ | ✅ | |
| SQLDescribeParam | ✅ | ✅ | ✅ | |
| SQLDisconnect | ✅ | ✅ | ✅ | |
| SQLDriverConnect | ✅ | ✅ | ✅ | |
| SQLEndTran | ✅ | ✅ | ✅ | TDengine is non-transactional, thus this is at most simulating |
| SQLExecDirect | ✅ | ✅ | ✅ | |
| SQLExecute | ✅ | ✅ | ✅ | |
| SQLExtendedFetch | ❌ | ❌ | ❌ | |
| SQLFetch | ✅ | ✅ | ✅ | |
| SQLFetchScroll | ✅ | ✅ | ✅ | TDengine has no counterpart, just implement SQL_FETCH_NEXT |
| SQLForeignKeys | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLFreeHandle | ✅ | ✅ | ✅ | |
| SQLFreeStmt | ✅ | ✅ | ✅ | |
| SQLGetConnectAttr | ✅ | ✅ | ✅ | Supports partial attributes; unsupported attributes are listed below. |
| SQLGetCursorName | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLGetData | ✅ | ✅ | ✅ | |
| SQLGetDescField | ❌ | ❌ | ❌ | |
| SQLGetDescRec | ❌ | ❌ | ❌ | |
| SQLGetDiagField | ✅ | ✅ | ✅ | |
| SQLGetDiagRec | ✅ | ✅ | ✅ | |
| SQLGetEnvAttr | ✅ | ✅ | ✅ | |
| SQLGetInfo | ✅ | ✅ | ✅ | |
| SQLGetStmtAttr | ✅ | ✅ | ✅ | Supports partial attributes; unsupported attributes are listed below. |
| SQLGetTypeInfo | ✅ | ✅ | ✅ | |
| SQLMoreResults | ✅ | ✅ | ✅ | |
| SQLNativeSql | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLNumParams | ✅ | ✅ | ✅ | |
| SQLNumResultCols | ✅ | ✅ | ✅ | |
| SQLParamData | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLPrepare | ✅ | ✅ | ✅ | |
| SQLPrimaryKeys | ✅ | ✅ | ✅ | |
| SQLProcedureColumns | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLProcedures | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLPutData | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLRowCount | ✅ | ✅ | ✅ | |
| SQLSetConnectAttr | ✅ | ✅ | ✅ | Supports partial attributes; unsupported attributes are listed below. |
| SQLSetCursorName | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLSetDescField | ❌ | ❌ | ❌ | |
| SQLSetDescRec | ❌ | ❌ | ❌ | |
| SQLSetEnvAttr | ✅ | ✅ | ✅ | |
| SQLSetPos | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLSetStmtAttr | ✅ | ✅ | ✅ | Supports partial attributes; unsupported attributes are listed below. |
| SQLSpecialColumns | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLStatistics | ❌ | ❌ | ❌ | TDengine has no counterpart |
| SQLTablePrivileges | ❌ | ❌ | ❌ | TDengine has no strict counterpart |
| SQLTables | ✅ | ✅ | ✅ | |

- **Non-supported-statement-attributes (SQLSetStmtAttr)**

| Attribute | Note |
| :----- | :---- |
| SQL_ATTR_CONCURRENCY | TDengine has no updatable-CURSOR mechanism |
| SQL_ATTR_FETCH_BOOKMARK_PTR | TDengine has no BOOKMARK mechanism |
| SQL_ATTR_IMP_PARAM_DESC | |
| SQL_ATTR_IMP_ROW_DESC | |
| SQL_ATTR_KEYSET_SIZE | |
| SQL_ATTR_PARAM_BIND_OFFSET_PTR | |
| SQL_ATTR_PARAM_OPERATION_PTR | |
| SQL_ATTR_ROW_NUMBER | Readonly attribute |
| SQL_ATTR_ROW_OPERATION_PTR | |
| SQL_ATTR_SIMULATE_CURSOR | |

- **Non-supported-connection-attributes (SQLSetConnectAttr)**

| Attribute | Note |
| :----- | :---- |
| SQL_ATTR_AUTO_IPD | Readonly attribute |
| SQL_ATTR_CONNECTION_DEAD | Readonly attribute |
| SQL_ATTR_ENLIST_IN_DTC | |
| SQL_ATTR_PACKET_SIZE | |
| SQL_ATTR_TRACE | |
| SQL_ATTR_TRACEFILE | |
| SQL_ATTR_TRANSLATE_LIB | |
| SQL_ATTR_TRANSLATE_OPTION | |

- **Enable any programming language with ODBC-bindings/ODBC-plugings to communicate with TDengine:**

| programming language | ODBC-API or bindings/plugins |
| :----- | :---- |
| C/C++ | ODBC-API |
| CSharp | System.Data.Odbc |
| Erlang | odbc module |
| Go | [odbc](https://github.com/alexbrainman/odbc), database/sql |
| Haskell | HDBC, HDBC-odbc |
| Common Lisp | plain-odbc |
| Nodejs | odbc |
| Python3 | pyodbc |
| Rust | odbc |

### API Functional Categories

This section summarizes the ODBC API by functionality. For a complete ODBC API reference, please visit the [Microsoft Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc).

#### Data Source and Driver Management

- API: ConfigDSN
  - **Supported**: Yes (Windows only)
  - **Standard**: ODBC
  - **Function**: Configures data sources
  
- API: ConfigDriver
  - **Supported**: Yes (Windows only)
  - **Standard**: ODBC
  - **Function**: Used to perform installation and configuration tasks related to a specific driver
  
- API: ConfigTranslator
  - **Supported**: No
  - **Standard**: ODBC
  - **Function**: Used to parse the DSN configuration, translating or converting between DSN configuration and actual database driver configuration

#### Connecting to Data Sources

- API: SQLAllocHandle
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Allocates environment, connection, statement, or descriptor handles

- API: SQLConnect
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Connects to a specific driver using data source name, user ID, and password

- API: SQLDriverConnect
  - **Supported**: Yes
  - **Standard**: ODBC
  - **Function**: Connects to a specific driver using a connection string, supporting more connection information

- API: SQLBrowseConnect
  - **Supported**: No
  - **Standard**: ODBC
  - **Function**: Used to discover and enumerate the characteristics and property values required to connect to a data source. Each call to SQLBrowseConnect returns a successive level of properties and property values

- API: SQLAllocEnv
  - **Supported**: No
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.x function SQLAllocEnv has been replaced by SQLAllocHandle

- API: SQLAllocConnect
  - **Supported**: No
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.x function SQLAllocConnect has been replaced by SQLAllocHandle

#### Retrieving Information about Drivers and Data Sources

- API: SQLDataSources
  - **Supported**: No
  - **Standard**: ISO 92
  - **Function**: Returns a list of available data sources, handled by the driver manager

- API: SQLDrivers
  - **Supported**: No
  - **Standard**: ISO 92
  - **Function**: Returns a list of installed drivers and their properties, handled by the driver manager

- API: SQLGetInfo
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Returns detailed information about the database environment, such as database product name, driver name, SQL syntax features of the database, connection capabilities, etc.

- API: SQLGetFunctions
  - **Supported**: No
  - **Standard**: ISO 92
  - **Function**: Used to query the functions supported by the driver

- API: SQLGetTypeInfo
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Returns information about supported data types

#### Setting and Retrieving Driver Properties

- API: SQLSetConnectAttr
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Sets connection attributes, used to control the auto-commit mode when setting the SQL_ATTR_AUTOCOMMIT attribute

- API: SQLGetConnectAttr
  - **Supported**: Yes
  - **Standard**: ISO 92
  - **Function**: Returns the value of connection attributes

- API: SQLSetConnectOption
  - **Supported**: No
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.0 function SQLSetConnectOption has been replaced by SQLSetConnectAttr

- API: SQLGetConnectOption
  - **Supported**: Not supported
  - **Standard**: Deprecated
  - **Purpose**: In ODBC 3.x, the ODBC 2.0 function SQLSetConnectOption has been replaced by SQLGetConnectAttr

- API: SQLSetEnvAttr
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Sets attributes that control the environment

- API: SQLGetEnvAttr
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Returns the current settings of environment attributes

- API: SQLSetStmtAttr
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Sets attributes related to statements

- API: SQLGetStmtAttr
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Returns the current settings of statement attributes

- API: SQLSetStmtOption
  - **Supported**: Not supported
  - **Standard**: Deprecated
  - **Purpose**: In ODBC 3.x, the ODBC 2.0 function SQLSetStmtOption has been replaced by SQLSetStmtAttr

- API: SQLGetStmtOption
  - **Supported**: Not supported
  - **Standard**: Deprecated
  - **Purpose**: In ODBC 3.x, the ODBC 2.0 function SQLSetStmtOption has been replaced by SQLGetStmtAttr

#### Preparing SQL Requests

- API: SQLAllocStmt
  - **Supported**: Not supported
  - **Standard**: Deprecated
  - **Purpose**: In ODBC 3.x, the ODBC 2.x function SQLAllocStmt has been replaced by SQLAllocHandle

- API: SQLPrepare
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Used for pre-processing SQL statements, typically a step before SQLExecute

- API: SQLBindCol
  - **Supported**: Supported
  - **Standard**: ODBC
  - **Purpose**: Used to bind columns in the result set to application buffers

- API: SQLBindParameter
  - **Supported**: Supported
  - **Standard**: ODBC
  - **Purpose**: Used to bind parameters of an SQL statement to application buffers

- API: SQLGetCursorName
  - **Supported**: Not supported
  - **Standard**: ISO 92
  - **Purpose**: Returns the cursor name associated with a specified statement

- API: SQLSetCursorName
  - **Supported**: Not supported
  - **Standard**: ISO 92
  - **Purpose**: Sets the cursor name, allowing named cursors to be used in queries

- API: SQLSetScrollOptions
  - **Supported**: Not supported
  - **Standard**: ODBC
  - **Purpose**: Sets options that control cursor behavior

#### Submitting Requests

- API: SQLExecute
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Used to execute an SQL statement previously prepared by SQLPrepare

- API: SQLExecDirect
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Used to execute a string containing an SQL statement

- API: SQLNativeSql
  - **Supported**: Not supported
  - **Standard**: ODBC
  - **Purpose**: Used to convert an application-provided SQL statement into the native SQL syntax of the database driver

- API: SQLDescribeParam
  - **Supported**: Supported
  - **Standard**: ODBC
  - **Purpose**: Returns a description of a specific parameter in a statement

- API: SQLNumParams
  - **Supported**: Supported
  - **Standard**: ISO 92
  - **Purpose**: Used to query the number of parameters in a precompiled SQL statement

- API: SQLParamData
  - **Supported**: Not supported
  - **Standard**: ISO 92
  - **Purpose**: Used to retrieve the next parameter value from the parameter data stream

- API: SQLPutData
  - **Support**: Not supported
  - **Standard**: ISO 92
  - **Function**: When using stream input mode, it can be used to send data blocks to output parameters

#### Retrieving Results and Information About Results

- API: SQLRowCount
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Returns the number of rows affected by an insert or delete request

- API: SQLNumResultCols
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Returns the number of columns in the result set

- API: SQLDescribeCol
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Used to describe the attributes of columns in the result set. It provides information about the data type of the column, column name, maximum width of the column, number of decimal places, and whether it is nullable

- API: SQLColAttribute
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Returns descriptor information of columns in the result set, such as title, sorting rules, etc.

- API: SQLColAttributes
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.0 function SQLColAttributes has been replaced by SQLColAttribute

- API: SQLGetData
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: Used to retrieve data for a specific column from the current row in the result set

- API: SQLMoreResults
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: After executing SQL statements with multiple result sets (e.g., a batch or stored procedure), moves to the next result set

- API: SQLFetch
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Used to fetch the next row from the result set and return data for all bound columns

- API: SQLFetchScroll
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Used to fetch a specified set of data rows from the result set and return data for all bound columns

- API: SQLExtendedFetch
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, SQLExtendedFetch has been replaced by SQLFetchScroll

- API: SQLSetPos
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: Sets the cursor position in the rowset and allows the application to update rows in the dataset

- API: SQLBulkOperations
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Performs bulk insert and bulk bookmark operations, including updates, deletions, and fetching by bookmark

#### Retrieving Error or Diagnostic Information

- API: SQLError
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.x function SQLError has been replaced by SQLGetDiagRec

- API: SQLGetDiagField
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Returns additional diagnostic information (single diagnostic result)

- API: SQLGetDiagRec
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Returns additional diagnostic information (multiple diagnostic results)

#### Retrieving Information About System Table Entries Related to the Data Source

- API: SQLColumnPrivileges
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Used to retrieve permission information for columns in a specified table, such as which users or roles have read, insert, update, or delete permissions on specific columns

- API: SQLColumns
  - **Support**: Supported
  - **Standard**: X/Open
  - **Function**: Returns a list of column names in a specified table

- API: SQLForeignKeys
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Retrieves detailed information about foreign key relationships

- API: SQLPrimaryKeys
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: Returns a list of column names that make up the primary key of a table

- API: SQLSpecialColumns
  - **Support**: Not supported
  - **Standard**: X/Open
  - **Function**: Returns information about special columns in the database, such as unique keys or index columns

- API: SQLStatistics
  - **Support**: Not supported
  - **Standard**: ISO 92
  - **Function**: Returns statistical information about tables, such as number of rows, number of columns, average row width, etc.

- API: SQLTablePrivileges
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Returns the privileges of a user on a specific table, such as SELECT, INSERT, UPDATE, etc.

- API: SQLTables
  - **Support**: Supported
  - **Standard**: X/Open
  - **Function**: Returns information about tables stored in the current database of the data source

- API: SQLProcedures
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Returns information about available stored procedures in the database, including names and types

- API: SQLProcedureColumns
  - **Support**: Not supported
  - **Standard**: ODBC
  - **Function**: Returns column information for stored procedures, including details of input and output parameters

#### Transaction Execution

- API: SQLTransact
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.x function SQLTransact has been replaced by SQLEndTran

- API: SQLEndTran
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Used to commit or rollback transactions. TDengine is non-transactional, so this function can at most simulate commit or rollback operations. If there are any outstanding connections or statements, neither commit nor rollback will succeed

#### Connection Termination

- API: SQLDisconnect
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Disconnects from the database

- API: SQLFreeHandle
  - **Support**: Supported
  - **Standard**: ISO 92
  - **Function**: Releases resources associated with a specific environment, connection, statement, or descriptor handle

- API: SQLFreeConnect
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.0 function SQLFreeConnect has been replaced by SQLFreeHandle

- API: SQLFreeEnv
  - **Support**: Not supported
  - **Standard**: Deprecated
  - **Function**: In ODBC 3.x, the ODBC 2.0 function SQLFreeEnv has been replaced by SQLFreeHandle

- API: SQLFreeStmt
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: Ends statement processing, discards pending results, and optionally releases all resources associated with the statement handle

- API: SQLCloseCursor
  - **Support**: Supported
  - **Standard**: ODBC
  - **Function**: Closes the cursor associated with the current statement handle and releases all resources used by the cursor

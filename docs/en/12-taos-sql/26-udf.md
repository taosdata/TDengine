---
title: User-Defined Functions (UDF)
sidebar_label: User-Defined Functions
description: This document describes the SQL statements related to user-defined functions (UDF) in TDengine.
---

You can create user-defined functions and import them into TDengine.
## Create UDF

SQL command can be executed on the host where the generated UDF DLL resides to load the UDF DLL into TDengine. This operation cannot be done through REST interface or web console. Once created, any client of the current TDengine can use these UDF functions in their SQL commands. UDF is stored in the management node of TDengine. The UDFs loaded in TDengine would be still available after TDengine is restarted.

When creating UDF, the type of UDF, i.e. a scalar function or aggregate function must be specified. If the specified type is wrong, the SQL statements using the function would fail with errors. The input data type and output data type must be consistent with the UDF definition.

- Create Scalar Function
```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type [LANGUAGE 'C|Python'];
```
  - OR REPLACE: if the UDF exists, the UDF properties are modified
  - function_name: The scalar function name to be used in the SQL statement
  - LANGUAGE 'C|Python': the programming language of UDF. Now C or Python (v3.7+) is supported. If this clause is omitted, C is assumed as the programming language.
  - library_path: For C programming language, The absolute path of the DLL file including the name of the shared object file (.so). For Python programming language, the absolute path of the Python UDF script. The path must be quoted with single or double quotes.
  - output_type: The data type of the results of the UDF.

  For example, the following SQL statement can be used to create a UDF from `libbitand.so`.

  ```sql
  CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
  ```
  For Example, the following SQL statement can be used to modify the existing function `bit_and`. The OUTPUT type is changed to BIGINT and the programming language is changed to Python.

  ```sql
  CREATE OR REPLACE FUNCTION bit_and AS "/home/taos/udf_example/bit_and.py" OUTPUTTYPE BIGINT LANGUAGE 'Python';
  ```

- Create Aggregate Function
```sql
CREATE AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ];
```
  - OR REPLACE: if the UDF exists, the UDF properties are modified
  - function_name: The aggregate function name to be used in the SQL statement
  - LANGUAGE 'C|Python': the programming language of the UDF. Now C or Python is supported. If this clause is omitted, C is assumed as the programming language.
  - library_path: For C programming language, The absolute path of the DLL file including the name of the shared object file (.so). For Python programming language, the absolute path of the Python UDF script. The path must be quoted with single or double quotes.
  - output_type: The output data type, the value is the literal string of the supported TDengine data type.
  - buffer_size: The size of the intermediate buffer in bytes. This parameter is optional.

  For example, the following SQL statement can be used to create a UDF from `libl2norm.so`.

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```
  For example, the following SQL statement modifies the buffer size of existing UDF `l2norm` to 64 
  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 64;
  ``` 

For more information about user-defined functions, see [User-Defined Functions](https://docs.tdengine.com/develop/udf/).

## Manage UDF

- The following statement deleted the specified user-defined function.
```
DROP FUNCTION function_name;
```

- function_name: The value of function_name in the CREATE statement used to import the UDF for example `bit_and` or `l2norm`. 
```sql
DROP FUNCTION bit_and;
```
- Show Available UDF
```sql
SHOW FUNCTIONS;
```

## Call UDF

The function name specified when creating UDF can be used directly in SQL statements, just like built-in functions. For example:
```sql
SELECT bit_and(c1,c2) FROM table;
```

The above SQL statement invokes function X for columns c1 and c2 on the table. You can use query keywords like WHERE with user-defined functions.

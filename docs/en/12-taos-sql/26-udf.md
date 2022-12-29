---
sidebar_label: User-Defined Functions
title: User-Defined Functions (UDF)
---

You can create user-defined functions and import them into TDengine.
## Create UDF

SQL command can be executed on the host where the generated UDF DLL resides to load the UDF DLL into TDengine. This operation cannot be done through REST interface or web console. Once created, any client of the current TDengine can use these UDF functions in their SQL commands. UDF are stored in the management node of TDengine. The UDFs loaded in TDengine would be still available after TDengine is restarted.

When creating UDF, the type of UDF, i.e. a scalar function or aggregate function must be specified. If the specified type is wrong, the SQL statements using the function would fail with errors. The input data type and output data type must be consistent with the UDF definition.

- Create Scalar Function
```sql
CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type;
```

  - function_name: The scalar function name to be used in SQL statement which must be consistent with the UDF name and is also the name of the compiled DLL (.so file).
  - library_path: The absolute path of the DLL file including the name of the shared object file (.so). The path must be quoted with single or double quotes.
  - output_type: The data type of the results of the UDF.

  For example, the following SQL statement can be used to create a UDF from `libbitand.so`.

  ```sql
  CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
  ```

- Create Aggregate Function
```sql
CREATE AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ];
```

  - function_name: The aggregate function name to be used in SQL statement which must be consistent with the udfNormalFunc name and is also the name of the compiled DLL (.so file).
  - library_path: The absolute path of the DLL file including the name of the shared object file (.so). The path must be quoted with single or double quotes.
  - output_type: The output data type, the value is the literal string of the supported TDengine data type.
  - buffer_size: The size of the intermediate buffer in bytes. This parameter is optional.

  For example, the following SQL statement can be used to create a UDF from `libl2norm.so`.

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```
For more information about user-defined functions, see [User-Defined Functions](../../develop/udf).

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

The function name specified when creating UDF can be used directly in SQL statements, just like builtin functions. For example:
```sql
SELECT bit_and(c1,c2) FROM table;
```

The above SQL statement invokes function X for column c1 and c2 on table. You can use query keywords like WHERE with user-defined functions.

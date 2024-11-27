---
title: Manage UDFs
description: Detailed guide on using UDFs
slug: /tdengine-reference/sql-manual/manage-udfs
---

In addition to the built-in functions of TDengine, users can also write their own function logic and incorporate it into the TDengine system.

## Creating UDF

Users can load UDF libraries from the client host into the system using SQL commands (this process cannot be done via RESTful interfaces or HTTP management interfaces). Once successfully created, all users in the current TDengine cluster can use these functions in SQL commands. UDFs are stored on the system's MNode, so even if the TDengine system is restarted, the created UDFs remain available.

When creating a UDF, it is important to distinguish between scalar functions and aggregate functions. If the incorrect function type is declared at creation, it may cause errors when calling the function via SQL commands. Additionally, users need to ensure that the input data types match the UDF program and that the UDF output data type matches the OUTPUTTYPE.

- Creating Scalar Functions

  ```sql
  CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type [LANGUAGE 'C|Python'];
  ```

  - OR REPLACE: If the function already exists, it will modify the attributes of the existing function.
  - function_name: The name of the scalar function that will be called in SQL in the future.
  - LANGUAGE 'C|Python': The programming language of the function, currently supporting C and Python. If this clause is omitted, the programming language is C.
  - library_path: If the programming language is C, the path is the absolute path of the dynamic link library containing the UDF function implementation (referring to the library file's save path on the current client host, usually pointing to a .so file). If the programming language is Python, the path is the path to the Python file containing the UDF function implementation. This path should be enclosed in single or double quotes.
  - output_type: The data type name of the function's calculation result.

For example, the following statement can create libbitand.so as a UDF available in the system:

```sql
CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
```

For instance, using the following statement, you can modify the already defined bit_and function to have an output type of BIGINT and implemented in Python.

```sql
CREATE OR REPLACE FUNCTION bit_and AS "/home/taos/udf_example/bit_and.py" OUTPUTTYPE BIGINT LANGUAGE 'Python';
```

- Creating Aggregate Functions:

  ```sql
  CREATE [OR REPLACE] AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ] [LANGUAGE 'C|Python'];
  ```

  - OR REPLACE: If the function already exists, it will modify the attributes of the existing function.
  - function_name: The name of the aggregate function that will be called in SQL in the future, must match the actual name of udfNormalFunc in the function implementation.
  - LANGUAGE 'C|Python': The programming language of the function, currently supporting C and Python (v3.7+).
  - library_path: If the programming language is C, the path is the absolute path of the dynamic link library containing the UDF function implementation (referring to the library file's save path on the current client host, usually pointing to a .so file). If the programming language is Python, the path is the path to the Python file containing the UDF function implementation. This path should be enclosed in single or double quotes.
  - output_type: The data type name of the function's calculation result.
  - buffer_size: The buffer size for intermediate calculation results, in bytes. If not used, it can be omitted.

For example, the following statement can create libl2norm.so as a UDF available in the system:

```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

For instance, using the following statement, you can modify the already defined l2norm function to have a buffer size of 64.

```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 64;
```

For information on how to develop custom functions, please refer to the [UDF User Guide](../../../developer-guide/user-defined-functions/).

## Managing UDF

- To delete a user-defined function with a specified name:

  ```sql
  DROP FUNCTION function_name;
  ```

- function_name: The meaning of this parameter is the same as in the CREATE command, i.e., the name of the function to be deleted, such as bit_and, l2norm.

  ```sql
  DROP FUNCTION bit_and;
  ```

- To display all UDFs currently available in the system:

  ```sql
  SHOW FUNCTIONS;
  ```

## Calling UDF

In SQL commands, you can directly call the user-defined function using the function name assigned when the UDF was created in the system. For example:

```sql
SELECT bit_and(c1,c2) FROM table;
```

This indicates calling the user-defined function named bit_and on the data columns named c1 and c2 in the table. User-defined functions can be used with query features like WHERE in SQL commands.

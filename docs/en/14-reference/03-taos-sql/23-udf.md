---
title: UDFs
slug: /tdengine-reference/sql-manual/manage-udfs
---

In addition to the built-in functions of TDengine, users can also write their own function logic and integrate it into the TDengine system.

## Creating UDFs

Users can load UDF libraries located on the client host into the system via SQL commands (this process cannot be done through the RESTful interface or HTTP management interface). Once created, all users in the current TDengine cluster can use these functions in SQL commands. UDFs are stored on the system's MNode nodes, so even if the TDengine system is restarted, the created UDFs are still available.

When creating UDFs, it is necessary to distinguish between scalar functions and aggregate functions. If the wrong function category is declared during creation, it may cause errors when calling the function via SQL commands. Additionally, users need to ensure that the input data type matches the UDF program, and the UDF output data type matches OUTPUTTYPE.

- Creating scalar functions

```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type [LANGUAGE 'C|Python'];
```

- OR REPLACE: If the function already exists, it will modify the existing function properties.
- function_name: The function name to be called in SQL for scalar functions;
- LANGUAGE 'C|Python': The programming language of the function, currently supports C and Python. If this clause is omitted, the programming language is C.
- library_path: If the programming language is C, the path is the absolute path of the library file containing the UDF function implementation (referring to the path where the library file is stored on the client host, usually pointing to a .so file). If the programming language is Python, the path is the Python file path containing the UDF function implementation. This path needs to be enclosed in single or double quotes;
- output_type: The data type name of this function's computation result;

For example, the following statement can create libbitand.so as a UDF available in the system:

  ```sql
  CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
  ```

For example, the following statement can modify the already defined bit_and function, with the output type as BIGINT, implemented in Python.

  ```sql
  CREATE OR REPLACE FUNCTION bit_and AS "/home/taos/udf_example/bit_and.py" OUTPUTTYPE BIGINT LANGUAGE 'Python';
  ```

- Creating aggregate functions:

```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ] [LANGUAGE 'C|Python'];
```

- OR REPLACE: If the function already exists, it will modify the existing function properties.
- function_name: The function name to be called in SQL for aggregate functions, must be consistent with the actual name of udfNormalFunc in the function implementation;
- LANGUAGE 'C|Python': The programming language of the function, currently supports C and Python (v3.7+).
- library_path: If the programming language is C, the path is the absolute path of the library file containing the UDF function implementation (referring to the path where the library file is stored on the client host, usually pointing to a .so file). If the programming language is Python, the path is the Python file path containing the UDF function implementation. This path needs to be enclosed in single or double quotes;;
- output_type: The data type name of this function's computation result;
- buffer_size: The buffer size for intermediate computation results, in bytes. If not used, it can be omitted.

  For example, the following statement can create libl2norm.so as a UDF available in the system:

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```

  For example, the following statement can modify the buffer size of the already defined l2norm function to 64.

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 64;
  ```

About how to develop custom functions, please refer to [UDF Usage Instructions](/developer-guide/user-defined-functions/).

## Manage UDF

- Delete a user-defined function with a specified name:

```sql
DROP FUNCTION function_name;
```

- function_name: The meaning of this parameter is consistent with the function_name parameter in the CREATE command, that is, the name of the function to be deleted, such as bit_and, l2norm

```sql
DROP FUNCTION bit_and;
```

- Display all the UDFs currently available in the system:

```sql
SHOW FUNCTIONS;
```

## Invoke UDF

In SQL commands, you can directly call the user-defined function using the function name assigned when the UDF was created in the system. For example:

```sql
SELECT bit_and(c1,c2) FROM table;
```

This means calling the user-defined function named bit_and on the data columns named c1 and c2 in the table. User-defined functions in SQL commands can be used in conjunction with query features such as WHERE.

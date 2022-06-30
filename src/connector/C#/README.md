## CSharp Connector

* This C# connector supports: Linux 64/Windows x64/Windows x86.
* This C# connector can be downloaded and included as a normal package from [Nuget.org](https://www.nuget.org/packages/TDengine.Connector/).

### Installation preparations

* Install TDengine client.
* .NET interface file TDengineDriver.cs and reference samples both 
  are located under Windows client's installation path:install_directory/examples/C#.
* Install [.NET SDK](https://dotnet.microsoft.com/download)

### Installation verification

Run {client_installation_directory}/examples/C#/C#Checker/C#Checker.cs

```cmd
cd {client_install_directory}/examples/C\#/C#Checker
//run c#checker.cs
dotnet run -- -h <FQDN>
```

### Example Source Code

You can find examples under follow directories:

* {client_installation_directory}/examples/C#
* [github C# example source code](https://github.com/taosdata/TDengine/tree/develop/tests/examples/C%23)

**Tips:**
"TDengineTest" is an example that includes some basic sample code like
connect, query and so on.

### Use C# connector

#### **prepare**

**tips:** Need to install .NET SDK first.

* Create a dotnet project(using console project as an example).

``` cmd
mkdir test
cd test
dotnet new console
```

* Add "TDengine.Connector" as a package through Nuget into project.

``` cmd
dotnet add package TDengine.Connector
```

#### **Connection**

``` C#
using TDengineDriver;
using System.Runtime.InteropServices;
// ... do something ...
string host = "127.0.0.1" ; 
string configDir =  "C:/TDengine/cfg"; // For linux should it be /etc/taos.
string user = "root";
string password = "taosdata";
string db = ''; // Also can set it to the db name you want to connect.
string port = 0

/* Set client options (optional step):charset, locale, timezone.
 * Default: charset, locale, timezone same to system.
 * Current supports options:TSDB_OPTION_LOCALE, TSDB_OPTION_CHARSET, TSDB_OPTION_TIMEZONE, TSDB_OPTION_CONFIGDIR.
*/
TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR,configDir);

// Get an TDengine connection
InPtr conn = TDengine.Connect(host, user, taosdata, db, port);

// Check if get connection success
if (conn == IntPtr.Zero)
{
   Console.WriteLine("Connect to TDengine failed");
}
else
{
   Console.WriteLine("Connect to TDengine success");
}

// Close TDengine Connection
if (conn != IntPtr.Zero)
{
    TDengine.Close(this.conn);
}

// Suggest to clean environment, before exit your application.
TDengine.Cleanup();
```

#### **Execute SQL**

```C#
// Suppose conn is a valid tdengine connection from previous Connection sample
public static void ExecuteSQL(IntPtr conn, string sql)
{
    IntPtr res = TDengine.Query(conn, sql);
    // Check if query success
    if((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
    {
        Console.Write(sql + " failure, ");
        // Get error message while Res is a not null pointer.
        if (res != IntPtr.Zero)
         {
             Console.Write("reason:" + TDengine.Error(res));
         }
    }
    else
    {
        Console.Write(sql + " success, {0} rows affected", TDengine.AffectRows(res));
        //... do something with res ...

        // Important: need to free result to avoid memory leak.
        TDengine.FreeResult(res);
    }
}

// Calling method to execute sql;
ExecuteSQL(conn,$"create database if not exists {db};");
ExecuteSQL(conn,$"use {db};");
string createSql = "CREATE TABLE meters(ts TIMESTAMP, current FLOAT,"+
" voltage INT, phase FLOAT)TAGS(location BINARY(30), groupId INT);"
ExecuteSQL(conn,createSql);
ExecuteSQL(conn," INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');");
ExecuteSQL(conn,$"drop database if exists {db};");
```

#### **Get Query Result**

```C#
// Following code is a sample that traverses retrieve data from TDengine.
public void ExecuteQuery(IntPtr conn,string sql)
{
    // "conn" is a valid TDengine connection which can
    // be got from previous "Connection" sample.
    IntPrt res = TDengine.Query(conn, sql);
    if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
    {
         Console.Write(sql.ToString() + " failure, ");
         if (res != IntPtr.Zero)
         {
             Console.Write("reason: " + TDengine.Error(res));
         }
         // Execute query sql failed
         // ... do something ...
    }

    // Retrieve data successfully then iterate through "res".

    // Fields count, num of fields, that is equal to retrieved column count.
    int fieldCount = TDengine.FieldCount(res);
    Console.WriteLine("field count: " + fieldCount);

    // Get query result field information in list form.
    List<TDengineMeta> metas = TDengine.FetchFields(res);
    for(int j = 0; j < metas.Count; j++)
    {
        TDengineMeta meta = (TDengineMeta)metas[j];
        Console.WriteLine($"index:{j},type:{meta.type},typename:{meta.TypeName()},name:{meta.name},size:{meta.size}");
    }

    // Iterate over the data from the retrieved results.
    IntPtr rowdata;
    StringBuilder builder = new StringBuilder();
    while ((rowdata = TDengine.FetchRows(res)) != IntPtr.Zero)
    {
        queryRows++;
        IntPtr colLengthPtr = TDengine.FetchLengths(res);
        int[] colLengthArr = new int[fieldCount];
        Marshal.Copy(colLengthPtr, colLengthArr, 0, fieldCount);

        for (int fields = 0; fields < fieldCount; ++fields)
        {
            TDengineMeta meta = metas[fields];
            int offset = IntPtr.Size * fields;
            IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

            builder.Append("---");

            if (data == IntPtr.Zero)
            {
                builder.Append("NULL");
                continue;
            }
            switch ((TDengineDataType)meta.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                    builder.Append(v1.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    sbyte v2 = (sbyte)Marshal.ReadByte(data);
                    builder.Append(v2.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    short v3 = Marshal.ReadInt16(data);
                    builder.Append(v3.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    int v4 = Marshal.ReadInt32(data);
                    builder.Append(v4.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    long v5 = Marshal.ReadInt64(data);
                    builder.Append(v5.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                    builder.Append(v6.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                    builder.Append(v7.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    string v8 = Marshal.PtrToStringUTF8(data, colLengthArr[fields]);
                    builder.Append(v8);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    long v9 = Marshal.ReadInt64(data);
                    builder.Append(v9.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    string v10 = Marshal.PtrToStringUTF8(data, colLengthArr[fields]);
                    builder.Append(v10);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    byte v12 = Marshal.ReadByte(data);
                    builder.Append(v12.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    ushort v13 = (ushort)Marshal.ReadInt16(data);
                    builder.Append(v13.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    uint v14 = (uint)Marshal.ReadInt32(data);
                    builder.Append(v14.ToString());
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    ulong v15 = (ulong)Marshal.ReadInt64(data);
                    builder.Append(v15.ToString());
                    break;
                default:
                    builder.Append("unknown value");
                    break;
            }
        }
        builder.Append("---");
     }
     // Do something with the result data, like print.
     Console.WriteLine(builder.ToString());

    // Important free "res".
     TDengine.FreeResult(res);
}
```

#### **Stmt Bind Sample**

* Bind different types of data.

```C#
// Prepare tags values used to binding by stmt.
// An instance of TAOS_BIND can just bind a cell of table.
TAOS_BIND[] binds = new TAOS_BIND[1];
binds[0] = TaosBind.BindNchar("-123acvnchar");
// Use TaosBind.BindNil() to bind null values.

long[] tsArr = new long[5] { 1637064040000, 1637064041000,
1637064042000, 1637064043000, 1637064044000 };
bool?[] boolArr = new bool?[5] { true, false, null, true, true };
int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null,
1000, long.MaxValue };
string[] binaryArr = new string[5] { "/TDengine/src/client/src/tscPrepare.c",
 String.Empty, null, "doBindBatchParam",
 "string.Jion:1234567890123456789012345" };

// TAOS_MULTI_BIND can bind a column of data.
TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[5];

mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);

// After using instance of TAOS_MULTI_BIND and TAOS_BIND,
// need to free the allocated unmanaged memory.
TaosMultiBind.FreeBind(mBind);
TaosMultiBind.FreeMBind(mBinds);
```

* Insert

```C#
  /* Pre-request: create stable or normal table.
   * Target table for this sample：stmtdemo
   * Structure：create stable stmtdemo (ts timestamp,b bool,v4 int,
   * v8 bigint,bin binary(100))tags(blob nchar(100));
  */
  // This conn should be a valid connection that is returned by TDengine.Connect().
  IntPtr conn;
  IntPtr stmt = IntPtr.Zero;
  // Insert statement
  string sql = "insert into ? using stmtdemo tags(?,?,?,?,?) values(?)";
  // "use db" before stmtPrepare().

  stmt = TDengine.StmtInit(conn);
  TDengine.StmtPrepare(stmt, sql);

  // Use method StmtSetTbname() to config tablename,
  // but needs to create the table before.
  // Using StmtSetTbnameTags() to config table name and
  // tags' value.(create sub table based on stable automatically)
  TDengine.StmtSetTbname_tags(stmt,"t1",binds);

  // Binding multiple lines of data.
  TDengine.StmtBindParamBatch(stmt,mBinds);

  // Add current bind parameters into batch.
  TDengine.StmtAddBatch(stmt);

  // Execute the batch instruction which has been prepared well by bind_param() method.
  TDengine.StmtExecute(stmt);

  // Cause we use unmanaged memory, remember to free occupied memory, after execution.
  TaosMultiBind.FreeBind(mBind);
  TaosMultiBind.FreeMBind(mBinds);

  // Get error information if current stmt operation failed.
  // This method is appropriate for all the stmt methods to get error message.
  TDengine.StmtError(stmt);
```

* Query

``` C#
stmt = StmtInit(conn);

string querySql = "SELECT * FROM T1 WHERE V4 > ? AND V8 < ?";
StmtPrepare(stmt, querySql);

// Prepare Query parameters.
TAOS_BIND qparams[2];
qparams[0] = TaosBind.bindInt(-2);
qparams[1] = TaosBind.bindLong(4);

// Bind parameters.
TDengine.StmtBindParam(stmt, qparams);

// Execute
TDengine.StmtExecute(stmt);

// Get querying result, for SELECT only.
// User application should be freed with API FreeResult() at the end.
IntPtr result = TDengine.StmtUseResult(stmt);

// This "result" cam be traversed as normal sql query result.
// ... Do something with "result" ...

TDengine.FreeResult(result);

// Cause we use unmanaged memory, we need to free occupied memory after execution.
TaosMultiBind.FreeBind(qparams);

// Close stmt and release resource.
TDengine.StmtClose(stmt);
```

* Assert (samples about how to assert every step of stmt is successed or failed)

```C#
// Special  StmtInit().
IntPtr stmt = TDengine.StmtInit(conn);
if ( stmt == IntPtr.Zero)
{
       Console.WriteLine("Init stmt failed:{0}",TDengine.StmtErrorStr(stmt));
       // ... do something ...
}
else
{
      Console.WriteLine("Init stmt success");
      // Continue
}

// For all stmt methods that return int type,we can get error message by StmtErrorStr().
if (TDengine.StmtPrepare(this.stmt, sql) == 0)
{
    Console.WriteLine("stmt prepare success");
    // Continue
}
else
{
     Console.WriteLine("stmt prepare failed:{0} " , TDengine.StmtErrorStr(stmt));
     // ... do something ...
}

// Estimate wether StmtUseResult() is successful or failed.
// If failed, get the error message by TDengine.Error(res)
IntPtr res = TDengine.StmtUseResult(stmt);
if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
{
      Console.Write( " StmtUseResult failure, ");
      if (res != IntPtr.Zero) {
        Console.Write("reason: " + TDengine.Error(res));
       }
}
else
{
 Console.WriteLine(sql.ToString() + " success");
}
```

* More samples reference from [examples](https://github.com/taosdata/TDengine/tree/develop/tests/examples/C%23/).

**Note:**

* TDengine V2.0.3.0 supports both 32-bit and 64-bit Windows systems,
  so when .NET project generates a .exe file, please select correspond
  with "X86" or "x64" for the "Platform" under "Solution"/"Project".
* This .NET interface has been verified in Visual Studio 2015/2017,
  other VS versions have not been verified yet.
* Since this. NET connector interface requires the taos.dll file, so before
  executing the application, copy the taos.dll file in the
  Windows {client_install_directory}/driver directory to the folder where the
  .NET project finally generated the .exe executable file. After running the exe
  file, you can access the TDengine database and do operations such as insert
  and query(This step can be skip if the client has been installed on you machine).

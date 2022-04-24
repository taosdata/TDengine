/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

/// <summary>
/// TDengine C# Driver.
/// </summary>
namespace TDengineDriver
{
    /// <summary>
    /// The enum indicate the data types and their code that TDengine supports.
    /// </summary>
    public enum TDengineDataType
    {
        TSDB_DATA_TYPE_NULL = 0,     // 1 bytes
        TSDB_DATA_TYPE_BOOL = 1,     // 1 bytes
        TSDB_DATA_TYPE_TINYINT = 2,  // 1 bytes
        TSDB_DATA_TYPE_SMALLINT = 3, // 2 bytes
        TSDB_DATA_TYPE_INT = 4,      // 4 bytes
        TSDB_DATA_TYPE_BIGINT = 5,   // 8 bytes
        TSDB_DATA_TYPE_FLOAT = 6,    // 4 bytes
        TSDB_DATA_TYPE_DOUBLE = 7,   // 8 bytes
        TSDB_DATA_TYPE_BINARY = 8,   // string
        TSDB_DATA_TYPE_TIMESTAMP = 9,// 8 bytes
        TSDB_DATA_TYPE_NCHAR = 10,   // Unicode string
        TSDB_DATA_TYPE_UTINYINT = 11,// 1 byte
        TSDB_DATA_TYPE_USMALLINT = 12,// 2 bytes
        TSDB_DATA_TYPE_UINT = 13,    // 4 bytes
        TSDB_DATA_TYPE_UBIGINT = 14,   // 8 bytes
        TSDB_DATA_TYPE_JSONTAG = 15   //4096 bytes 
    }

    /// <summary>
    /// Options that can be set before get TDegnine connection. Can set like locale,char-set,timezone,taos.cfg Dir 
    /// and connection active time.
    /// </summary>
    public enum TDengineInitOption
    {
        TSDB_OPTION_LOCALE = 0,
        TSDB_OPTION_CHARSET = 1,
        TSDB_OPTION_TIMEZONE = 2,
        TSDB_OPTION_CONFIGDIR = 3,
        TSDB_OPTION_SHELL_ACTIVITY_TIMER = 4
    }

    /// <summary>
    /// This enum is used to indicate different TDengine's different schemaless protocol.
    /// </summary>
    public enum TDengineSchemalessProtocol
    {
        TSDB_SML_UNKNOWN_PROTOCOL = 0,
        /// <summary>
        /// Same as InfluxDB's line protocol.
        /// </summary>
        TSDB_SML_LINE_PROTOCOL = 1,
        /// <summary>
        /// Same as OpenTSDB's telnet protocol.
        /// </summary>
        TSDB_SML_TELNET_PROTOCOL = 2,
        /// <summary>
        /// Same as OpenTSDB's json protocol,can insert json format data.
        /// </summary>
        TSDB_SML_JSON_PROTOCOL = 3
    }

    /// <summary>
    /// Precision string of the timestamps in the text data while using schemaless insert.
    /// </summary>
    public enum TDengineSchemalessPrecision
    {
        TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
        TSDB_SML_TIMESTAMP_HOURS = 1,
        TSDB_SML_TIMESTAMP_MINUTES = 2,
        TSDB_SML_TIMESTAMP_SECONDS = 3,
        TSDB_SML_TIMESTAMP_MILLI_SECONDS = 4,
        TSDB_SML_TIMESTAMP_MICRO_SECONDS = 5,
        TSDB_SML_TIMESTAMP_NANO_SECONDS = 6
    }
    enum TaosField
    {
        STRUCT_SIZE = 68,
        NAME_LENGTH = 65,
        TYPE_OFFSET = 65,
        BYTES_OFFSET = 66,
    }

    /// <summary>
    /// TDengine's meta info.
    /// </summary>
    public class TDengineMeta
    {
        /// <summary>
        /// Retrieve result's column name.
        /// </summary>
        public string name;

        /// <summary>
        /// Column's length.(Unit bytes)
        /// </summary>
        public short size;

        /// <summary>
        /// Column type code from retrieved result. Correspond with <see cref="TDengineDataType"/>
        /// </summary>
        public byte type;

        /// <summary>
        /// Get the type name from retrieved result. 
        /// </summary>
        /// <returns></returns>
        public string TypeName()
        {
            switch ((TDengineDataType)type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    return "BOOL";
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    return "TINYINT";
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    return "SMALLINT";
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    return "INT";
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    return "BIGINT";
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    return "TINYINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    return "SMALLINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    return "INT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    return "BIGINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    return "FLOAT";
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    return "DOUBLE";
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    return "BINARY";
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    return "TIMESTAMP";
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    return "NCHAR";
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    return "JSON";
                default:
                    return "undefined";
            }
        }
    }

    /// <summary>
    /// <c>TAOS_BIND</c> struct, used to hold a parameter(one value) while using "stmt insert".
    /// </summary>
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public struct TAOS_BIND
    {
        /// <summary>
        /// Indicate the <see cref="TDengineDataType"/> of data you want to bind.
        /// </summary>
        public int buffer_type;

        /// <summary>
        /// A reference point to the value you want to bind. 
        /// </summary>
        public IntPtr buffer;

        /// <summary>
        /// Unused,but need to allocate space.
        /// </summary>
        public Int32 buffer_length;

        /// <summary>
        /// Actual value length in <see cref="buffer"/>.
        /// </summary>        
        public IntPtr length;

        /// <summary>
        /// A reference to an variable to indicate if the column value is null or not
        /// </summary>
        public IntPtr is_null;
        
        /// <summary>
        /// Unused,but need to allocate space.
        /// </summary>
        public int is_unsigned;

        /// <summary>
        /// Unused,but need to allocate space.
        /// </summary>
        public IntPtr error;

        /// <summary>
        /// Unused,but need to allocate space.
        /// </summary>
        public Int64 u;

        /// <summary>
        /// Unused,but need to allocate space.
        /// </summary>
        public uint allocated;
    }

    /// <summary>
    /// <c>TAOS_MULTI_BIND</c>struct,used to hold a multiple values while using "stmt bind". 
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct TAOS_MULTI_BIND
    {
        /// <summary>
        /// Indicate the <see cref="TDengineDataType"/> of data you want to bind.
        /// </summary>
        public int buffer_type;

        /// <summary>
        /// The reference point to the array which stores multiple lines column values you want to bind. 
        /// </summary>
        public IntPtr buffer;

        /// <summary>
        /// Actual value length in <see cref="buffer"/>.
        /// </summary> 
        public ulong buffer_length;

        /// <summary>
        /// A reference point to the array which stores actual data length for each value. 
        /// </summary>
        public IntPtr length;

        /// <summary>
        /// A reference point to the array which stores identifies to indicate each value is null or not.
        /// </summary>
        public IntPtr is_null;

        /// <summary>
        /// Line number or the count of values in buffer.
        /// </summary>
        public int num;
    }

    /// <summary>
    /// User defined callback function for interface <c><see cref="TDengine.QueryAsync(IntPtr, string, QueryAsyncCallback, IntPtr)"/></c>,actually is a delegate in .Net.
    /// This function aim to handle the <c>taoRes</c> which points to the caller method's SQL result set. 
    /// </summary>
    /// <param name="param">This parameter will sent by caller method (<see cref="TDengine.QueryAsync(IntPtr, string, QueryAsyncCallback, IntPtr)"/>)</param>
    /// <param name="taoRes">This is the retrieved by caller method's SQL.</param>
    /// <param name="code">0 for indicate operation success and negative for operation fail.</param>
    public delegate void QueryAsyncCallback(IntPtr param, IntPtr taoRes, int code);

    /// <summary>
    /// User defined callback function for interface <c><see cref="TDengine.FetchRowAsync(IntPtr, FetchRowAsyncCallback, IntPtr)"/></c>,actually is a delegate in .Net.
    /// This callback allow applications to get each row of the batch records by calling <c><see cref="TDengine.FetchRowAsync(IntPtr, FetchRowAsyncCallback, IntPtr)"/></c> 
    /// forward iteration.After reading all the records in a block, the application needs to continue calling 
    /// <c><see cref="TDengine.FetchRowAsync(IntPtr, FetchRowAsyncCallback, IntPtr)"/></c> in this callback function to obtain the next batch of records for 
    /// processing until the number of records is zero.
    /// </summary>
    /// <param name="param">The parameter passed by <c><see cref="TDengine.FetchRowAsync(IntPtr, FetchRowAsyncCallback, IntPtr)"/></c></param>
    /// <param name="taoRes">Query Result.</param>
    /// <param name="numOfRows"> The number of rows of data obtained (not a function of
    /// the entire query result set). When the number is zero (the result is returned) 
    /// or the number of records is negative (the query fails).</param>
    public delegate void FetchRowAsyncCallback(IntPtr param, IntPtr taoRes, int numOfRows);

    /// <summary>
    /// In asynchronous subscribe mode, the prototype of the callback function.
    /// </summary>
    /// <param name="subscribe">Subscription object return by <c><see cref="TDengine.Subscribe(IntPtr, bool, string, string, SubscribeCallback, IntPtr, int)"/></c></param>
    /// <param name="tasRes">Query retrieve result set. (Note there may be no record in the result set.)</param>
    /// <param name="param">Additional parameters supplied by the client when <c><see cref="TDengine.Subscribe(IntPtr, bool, string, string, SubscribeCallback, IntPtr, int)"/> is called.</c></param>
    /// <param name="code">Error code.</param>
    public delegate void SubscribeCallback(IntPtr subscribe, IntPtr tasRes, IntPtr param, int code);

    /// <summary>
    /// Defined this Driver's mainly APIs
    /// </summary>
    public class TDengine
    {
        /// <summary>
        /// The success code return by major of this <c><see cref="TDengineDriver"/></c>'s operators.
        /// </summary>
        public const int TSDB_CODE_SUCCESS = 0;

        /// <summary>
        /// Initialize the running environment. 
        /// If the application does not actively call the API, the API will be automatically called when the application call 
        /// <c><see cref="Connect"/></c>, so the application generally does not need to call the API manually.
        /// </summary>
        [DllImport("taos", EntryPoint = "taos_init", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Init();

        /// <summary>
        /// Clean up the running environment and call this API before the application exits.
        /// </summary>
        [DllImport("taos", EntryPoint = "taos_cleanup", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Cleanup();

        /// <summary>
        /// Set client options, currently only time zone setting (_TSDB_OPTIONTIMEZONE)and encoding setting (_TSDB_OPTIONLOCALE) are supported. 
        /// The time zone and encoding default to the current operating system settings.
        /// </summary>
        /// <param name="option"></param>
        /// <param name="value">When the return value is 0, it means success, and when it is -1, it means failure.</param>
        [DllImport("taos", EntryPoint = "taos_options", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Options(int option, string value);

        /// <summary>
        /// Create a database connection and initialize the connection context. The parameters that need to be provided by user.
        /// </summary>
        /// <param name="ip"> FQDN used by TDengine to manage the master node.</param>
        /// <param name="user">User name.</param>
        /// <param name="password">Password</param>
        /// <param name="db">Database name. If user does not provide it, it can be connected normally, 
        /// means user can create a new database through this connection. 
        /// If user provides a database name, means the user has created the database and the database is used by default</param>
        /// <param name="port">Port number</param>
        /// <returns>A null return value indicates a failure. 
        /// The application needs to save the returned parameters for subsequent API calls. 
        /// Note: The same process can connect to multiple taosd processes based on ip/port</returns>
        [DllImport("taos", EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr Connect(string ip, string user, string password, string db, short port);

        /// <summary>
        /// Private function.Get the reason why the last API call failed, and the return value is a string.
        /// Also see<seealso cref="Error"/>.
        /// </summary>
        /// <param name="res">Reference return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c></param>
        /// <returns>Error reason.</returns>
        [DllImport("taos", EntryPoint = "taos_errstr", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr taos_errstr(IntPtr res);

        /// <summary>
        /// Public function,calling <c><see cref="taos_errstr"/></c> inside.Get the reason why the last API call failed, and the return value is a string.
        /// Also see <c><seealso cref="taos_errstr"/></c>.
        /// </summary>
        /// <param name="res">Reference return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>Error reason.</returns>
        static public string Error(IntPtr res)
        {
            IntPtr errPtr = taos_errstr(res);
            return Marshal.PtrToStringAnsi(errPtr);
        }

        /// <summary>
        /// Get the reason why the last API call failed, and the return value is the error code.
        /// </summary>
        /// <param name="res">Reference return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>Error code.</returns>
        [DllImport("taos", EntryPoint = "taos_errno", CallingConvention = CallingConvention.Cdecl)]
        static extern public int ErrorNo(IntPtr res);

        /// <summary>
        /// Private function.
        /// This API is used to execute SQL statements, which can be DQL, DML or DDL statements.
        /// </summary>
        /// <param name="conn">The database connection returned by calling <c><see cref="Connect(string, string, string, string, short)"/></c>.</param>
        /// <param name="byteArr">The SQL statement needed to execute.</param>
        /// <returns>A reference point to the result.</returns>
        [DllImport("taos", EntryPoint = "taos_query", CallingConvention = CallingConvention.Cdecl)]
        // static extern public IntPtr Query(IntPtr conn, string sqlstr);
        static extern private IntPtr Query(IntPtr conn, IntPtr byteArr);

        /// <summary>
        /// Public function.
        /// This API is used to execute SQL statements, which can be DQL, DML or DDL statements.
        /// Change the SQL command to UTF-8 to avoid error under Windows.
        /// </summary>
        /// <param name="conn">The database connection returned by calling <c><see cref="Connect(string, string, string, string, short)"/></c></param>
        /// <param name="command">The SQL statement needed to execute.</param>
        /// <returns>A reference point to the result.</returns>
        static public IntPtr Query(IntPtr conn, string command)
        {
            IntPtr res = IntPtr.Zero;

            IntPtr commandBuffer = Marshal.StringToCoTaskMemUTF8(command);
            res = Query(conn, commandBuffer);
            Marshal.FreeCoTaskMem(commandBuffer);
            return res;
        }

        /// <summary>
        /// Get the number of rows affected by the executed SQL statement.
        /// </summary>
        /// <param name="res">Result return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>Number of row have been affected.</returns>
        [DllImport("taos", EntryPoint = "taos_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        static extern public int AffectRows(IntPtr res);

        /// <summary>
        /// Get the number of columns in the query result set.
        /// </summary>
        /// <param name="res">Result return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>Number of columns in the query result set</returns>
        [DllImport("taos", EntryPoint = "taos_field_count", CallingConvention = CallingConvention.Cdecl)]
        static extern public int FieldCount(IntPtr res);

        /// <summary>
        /// Get the attributes (data type, name, number of bytes) of each column of data in the query result set, 
        /// which can be used in conjunction with taos_num_files to parse the data of a tuple (one row) returned 
        /// by <c><see cref="FetchRows(IntPtr)"/></c>. 
        /// </summary>
        /// <param name="res">Result return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>A reference point to the attributes (data type, name, number of bytes) of each column of data in the query result set.</returns>
        [DllImport("taos", EntryPoint = "taos_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr taos_fetch_fields(IntPtr res);

        /// <summary>
        /// Get the attributes (data type, name, number of bytes) of each column of data in the query result set, 
        /// which can be used in conjunction with taos_num_files to parse the data of a tuple (one row) returned 
        /// by <c><see cref="FetchRows(IntPtr)"/></c>
        /// </summary>
        /// <param name="res">Result return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>A list of <see cref="TDengineMeta"/></returns>
        static public List<TDengineMeta> FetchFields(IntPtr res)
        {

            List<TDengineMeta> metaList = new List<TDengineMeta>();
            if (res == IntPtr.Zero)
            {
                return metaList;
            }

            int fieldCount = FieldCount(res);
            IntPtr fieldsPtr = taos_fetch_fields(res);

            for (int i = 0; i < fieldCount; ++i)
            {
                int offset = i * (int)TaosField.STRUCT_SIZE;
                TDengineMeta meta = new TDengineMeta();
                meta.name = Marshal.PtrToStringAnsi(fieldsPtr + offset);
                meta.type = Marshal.ReadByte(fieldsPtr + offset + (int)TaosField.TYPE_OFFSET);
                meta.size = Marshal.ReadInt16(fieldsPtr + offset + (int)TaosField.BYTES_OFFSET);
                metaList.Add(meta);
            }


            return metaList;
        }
        /// <summary>
        /// Get the data in the query result set by rows.
        /// </summary>
        /// <param name="res">Result return by APIs like <c><see cref="Query(IntPtr, IntPtr)"/></c>.</param>
        /// <returns>Reference point to the query result set.</returns>
        [DllImport("taos", EntryPoint = "taos_fetch_row", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr FetchRows(IntPtr res);

        /// <summary>
        /// Release the query result set and related resources. 
        /// After the query is completed, be sure to call the API to release resources, 
        /// otherwise it may lead to application memory leakage. However, 
        /// it should also be noted that after releasing resources, 
        /// if you call functions such as <c><see cref="Consume(IntPtr)"/></c> to obtain query results, 
        /// it will lead the application to Crash.
        /// </summary>
        /// <param name="res">Reference point to the query result set.</param>
        [DllImport("taos", EntryPoint = "taos_free_result", CallingConvention = CallingConvention.Cdecl)]
        static extern public void FreeResult(IntPtr res);

        /// <summary>
        /// Close the connection.
        /// </summary>
        /// <param name="taos">Pointer returned by <c><see cref="Connect(string, string, string, string, short)"/></c> function.</param>
        [DllImport("taos", EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Close(IntPtr taos);

        //get precision of restultset
        /// <summary>
        /// The precision of the timestamp field in the returned result set.
        /// </summary>
        /// <param name="taos">Pointer returned by <c><see cref="Connect(string, string, string, string, short)"/></c> function.</param>
        /// <returns> 0 for milliseconds, 1 for microseconds, and 2 for nanoseconds.</returns>
        [DllImport("taos", EntryPoint = "taos_result_precision", CallingConvention = CallingConvention.Cdecl)]
        static extern public int ResultPrecision(IntPtr taos);

        //schemaless API 
        /// <summary>
        /// In addition to writing data using SQL or using the parameter binding API, writing can also be done using Schemaless, 
        /// which eliminates the need to create a super table/data sub-table data structure in advance and writes data directly, 
        /// while the TDengine system automatically creates and maintains the required table structure based on the written data 
        /// content.
        /// </summary>
        /// <param name="taos">Database connection, the database connection established by <c><see cref="Connect(string, string, string, string, short)"/></c> function.</param>
        /// <param name="lines">A pattern-free text string that meets the parsing format requirements.</param>
        /// <param name="numLines">The number of lines of the text data, cannot be 0.</param>
        /// <param name="protocol">The protocol type <c><seealso cref="TDengineSchemalessProtocol"/></c> of the lines, used to identify the format of the text data.</param>
        /// <param name="precision">Precision <c><seealso cref="TDengineSchemalessPrecision"/></c> string of the timestamps in the text data.</param>
        /// <returns></returns>
        [DllImport("taos", SetLastError = true, EntryPoint = "taos_schemaless_insert", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr SchemalessInsert(IntPtr taos, string[] lines, int numLines, int protocol, int precision);

        //stmt APIs:
        /// <summary>
        /// init a TAOS_STMT object for later use.
        /// </summary>
        /// <param name="taos">a valid taos connection</param>
        /// <returns>
        /// Not NULL returned for success, NULL for failure. And it should be freed with taos_stmt_close. 
        /// </returns>
        [DllImport("taos", EntryPoint = "taos_stmt_init", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr StmtInit(IntPtr taos);

        /// <summary>
        /// prepare a sql statement，'sql' should be a valid INSERT/SELECT statement.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="sql">sql string,used to bind parameters with</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_prepare", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtPrepare(IntPtr stmt, string sql);

        /// <summary>
        /// For INSERT only. Used to bind table name as a parmeter for the input stmt object.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">table name you want to  bind</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_set_tbname", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtSetTbname(IntPtr stmt, string name);

        /// <summary>
        /// For INSERT only. 
        /// Set a table name for binding table name as parameter. Only used for binding all tables 
        /// in one stable, user application must call 'loadTableInfo' API to load all table 
        /// meta before calling this API. If the table meta is not cached locally, it will return error.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">table name which is belong to an stable</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_set_sub_tbname", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtSetSubTbname(IntPtr stmt, string name);

        /// <summary>
        /// For INSERT only.
        /// set a table name for binding table name as parameter and tag values for all  tag parameters. 
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">use to set table name</param>
        /// <param name="tags">
        /// is an array contains all tag values,each item in the array represents a tag column's value.
        ///  the item number and sequence should keep consistence with that in stable tag definition.
        /// </param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_set_tbname_tags", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtSetTbnameTags(IntPtr stmt, string name, TAOS_BIND[] tags);

        /// <summary>
        /// For both INSERT and SELECT.
        /// bind a whole line data.  
        /// The usage of structure TAOS_BIND is the same with MYSQL_BIND in MySQL.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="bind">
        /// points to an array contains the whole line data.
        /// the item number and sequence should keep consistence with columns in sql statement.
        /// </param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_bind_param", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        static extern public int StmtBindParam(IntPtr stmt, TAOS_BIND[] bind);

        /// <summary>
        /// bind a single column's data, INTERNAL used and for INSERT only. 
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="bind">points to a column's data which could be the one or more lines. </param>
        /// <param name="colIdx">the column's index in prepared sql statement, it starts from 0.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_bind_single_param_batch", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtBindSingleParamBatch(IntPtr stmt, ref TAOS_MULTI_BIND bind, int colIdx);

        /// <summary>
        /// for INSERT only
        /// bind one or multiple lines data. The parameter 'bind'  
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="bind">
        /// points to an array contains one or more lines data.Each item in array represents a column's value(s),
        /// the item number and sequence should keep consistence with columns in sql statement. 
        /// </param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_bind_param_batch", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtBindParamBatch(IntPtr stmt, [In, Out] TAOS_MULTI_BIND[] bind);

        /// <summary>
        /// For INSERT only.
        /// add all current bound parameters to batch process. Must be called after each call to 
        /// StmtBindParam/StmtBindSingleParamBatch, or all columns binds for one or more lines 
        /// with StmtBindSingleParamBatch. User application can call any bind parameter 
        /// API again to bind more data lines after calling to this API.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_add_batch", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtAddBatch(IntPtr stmt);

        /// <summary>
        /// actually execute the INSERT/SELECT sql statement. 
        /// User application can continue to bind new data after calling to this API.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns></returns>
        [DllImport("taos", EntryPoint = "taos_stmt_execute", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtExecute(IntPtr stmt);

        /// <summary>
        /// For SELECT only,getting the query result. User application should free it with API 'FreeResult' at the end.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>Not NULL for success, NULL for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_use_result", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr StmtUseResult(IntPtr stmt);

        /// <summary>
        /// close STMT object and free resources.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_close", CallingConvention = CallingConvention.Cdecl)]
        static extern public int StmtClose(IntPtr stmt);

        /// <summary> 
        /// user application must call this API to  load all tables meta.
        /// </summary>
        /// <param name="taos">taos connection</param>
        /// <param name="tableList">tablelist</param>
        /// <returns></returns>
        [DllImport("taos", EntryPoint = "taos_load_table_info", CallingConvention = CallingConvention.Cdecl)]
        static extern private int LoadTableInfoDll(IntPtr taos, string tableList);

        /// <summary>
        /// user application  call this API to load all tables meta,this method call the native
        /// method LoadTableInfoDll.
        /// this method must be called before StmtSetSubTbname(IntPtr stmt, string name);
        /// </summary>
        /// <param name="taos">taos connection</param>
        /// <param name="tableList">tables need to load meta info are form in an array</param>
        /// <returns></returns>
        static public int LoadTableInfo(IntPtr taos, string[] tableList)
        {
            string listStr = string.Join(",", tableList);
            return LoadTableInfoDll(taos, listStr);
        }

        /// <summary>
        /// get detail error message when got failure for any stmt API call. If not failure, the result 
        /// returned in this API is unknown.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>point the error message</returns>
        [DllImport("taos", EntryPoint = "taos_stmt_errstr", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr StmtErrPtr(IntPtr stmt);

        /// <summary>
        /// get detail error message when got failure for any stmt API call. If not failure, the result 
        /// returned in this API is unknown.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>error string</returns>
        static public string StmtErrorStr(IntPtr stmt)
        {
            IntPtr stmtErrPrt = StmtErrPtr(stmt);
            return Marshal.PtrToStringAnsi(stmtErrPrt);
        }

        [DllImport("taos", EntryPoint = "taos_fetch_lengths", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr FetchLengths(IntPtr taos);

        // Async Query 
        /// <summary>
        /// This API uses non-blocking call mode.
        /// Application can open multiple tables and manipulate(query or insert) opened table concurrently. 
        /// So applications must ensure that opetations on the same table is completely serialized.
        /// Because that will cause some query and insert operations cannot be performed.
        /// </summary>
        /// <param name="taos"> A taos connection return by Connect()</param>
        /// <param name="sql">sql command need to execute</param>
        /// <param name="fq">User-defined callback function. <see cref="QueryAsyncCallback"/></param>
        /// <param name="param">the parameter for callback</param>       
        [DllImport("taos", EntryPoint = "taos_query_a", CallingConvention = CallingConvention.Cdecl)]
        static extern public void QueryAsync(IntPtr taos, string sql, QueryAsyncCallback fq, IntPtr param);

        /// <summary>
        /// Get the result set of asynchronous queries in batch, 
        /// which can only be used with QueryAsync().<c>FetchRowAsyncCallback</c>
        /// </summary>
        /// <param name="taoRes"> The result set returned when backcall QueryAsyncCallback </param>
        /// <param name="fq"> Callback function.<see cref="FetchRowAsyncCallback"/></param>
        /// <param name="param"> The parameter for callback FetchRowAsyncCallback </param>
        [DllImport("taos", EntryPoint = "taos_fetch_rows_a", CallingConvention = CallingConvention.Cdecl)]
        static extern public void FetchRowAsync(IntPtr taoRes, FetchRowAsyncCallback fq, IntPtr param);

        // Subscribe

        /// <summary>
        /// This function is used for start subscription service.
        /// </summary>
        /// <param name="taos"> taos connection return by <see cref = "Connect"></see></param>
        /// <param name="restart">If the subscription is already exists, to decide whether to
        /// start over or continue with previous subscription.</param>
        /// <param name="topic"> The name of the subscription.(This is the unique identification of the subscription).</param>
        /// <param name="sql">The subscribe statement(select only).Only query original data and in positive time sequence.</param>
        /// <param name="fq">The callback function when the query result is received.</param>
        /// <param name="param"> Additional parameter when calling callback function. System API will pass it to
        /// callback function without any operations.It is only used when calling asynchronously,
        /// and this parameter should be passed to NULL when calling synchronously</param>
        /// <param name="interval">Polling period in milliseconds. During asynchronous call, the callback function will be
        /// called periodically according to this parameter; In order to avoid affecting system
        /// performance, it is not recommended to set this parameter too small; When calling synchronously,
        /// if the interval between two calls to taos_consume is less than this period, the API will block
        /// until the interval exceeds this period.</param>
        /// <returns>Return null for failure, return subscribe object for success.</returns>
        [DllImport("taos", EntryPoint = "taos_subscribe", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr Subscribe(IntPtr taos, int restart, string topic, string sql, SubscribeCallback fq, IntPtr param, int interval);

        /// <summary>
        ///  This function is used for start subscription service.
        /// </summary>
        /// <param name="taos"> taos connection return by <see cref = "Connect"></see></param>
        /// <param name="restart">If the subscription is already exists, to decide whether to
        /// start over or continue with previous subscription.</param>
        /// <param name="topic"> The name of the subscription.(This is the unique identification of the subscription).</param>
        /// <param name="sql">The subscribe statement(select only).Only query original data and in positive time sequence.</param>
        /// <param name="fq">The callback function <c><see cref="TDengineDriver.SubscribeCallback"/></c>when the query result is received.</param>
        /// <param name="param"> Additional parameter when calling callback function. System API will pass it to
        /// callback function without any operations.It is only used when calling asynchronously,
        /// and this parameter should be passed to NULL when calling synchronously</param>
        /// <param name="interval">Polling period in milliseconds. During asynchronous call, the callback function will be
        /// called periodically according to this parameter; In order to avoid affecting system
        /// performance, it is not recommended to set this parameter too small; When calling synchronously,
        /// if the interval between two calls to taos_consume is less than this period, the API will block
        /// until the interval exceeds this period.</param>
        /// <returns>Return null for failure, return subscribe object for success.</returns>
        /// <exception cref="Exception"></exception>
        static public IntPtr Subscribe(IntPtr taos, bool restart, string topic, string sql, SubscribeCallback fq, IntPtr param, int interval)
        {
            if (taos == IntPtr.Zero)
            {
                Console.WriteLine("taos connect is null,subscribe failed");
                throw new Exception("taos connect is null");
            }
            else
            {
                IntPtr subPtr = Subscribe(taos, restart == true ? 1 : 0, topic, sql, fq, param, interval);
                return subPtr;
            }
        }

        /// <summary>
        /// Only synchronous mode, this function is used to get the result of subscription.
        /// If the interval between two calls to taos_consume is less than the polling
        /// cycle of the subscription, the API will block until the interval exceeds this
        /// cycle. If a new record arrives in the database, the API will return the latest
        /// record, otherwise it will return an empty result set with no records.
        /// If the return value is NULL, it indicates a system error.
        /// </summary>
        /// <param name="subscribe"> Subscription object return by <c><see cref="Subscribe(IntPtr, bool, string, string, SubscribeCallback, IntPtr, int)"/></c> </param>
        /// <returns>A result set. The data retrieve by consumer.</returns>
        [DllImport("taos", EntryPoint = "taos_consume", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr TaosConsume(IntPtr subscribe);

        /// <summary>
        /// Only synchronous mode, this function is used to get the result of subscription.
        /// If the interval between two calls to taos_consume is less than the polling
        /// cycle of the subscription, the API will block until the interval exceeds this
        /// cycle. If a new record arrives in the database, the API will return the latest
        /// record, otherwise it will return an empty result set with no records.
        /// If the return value is NULL, it indicates a system error.
        /// </summary>
        /// <param name="subscribe"> Subscription object return by <c><see cref="Subscribe(IntPtr, bool, string, string, SubscribeCallback, IntPtr, int)"/></c> </param>
        /// <returns></returns>
        static public IntPtr Consume(IntPtr subscribe)
        {
            IntPtr res = IntPtr.Zero;
            if (subscribe == IntPtr.Zero)
            {
                Console.WriteLine("Object subscribe is null,please subscribe first.");
                throw new Exception("Object subscribe is null");
            }
            else
            {
                res = TaosConsume(subscribe);
            }
            return res;
        }

        /// <summary>
        /// Unsubscribe.
        /// </summary>
        /// <param name="subscribe"> Subscription object return by "Subscribe" </param>
        /// <param name="keep"> If it is not 0, the API will keep the progress of subscription,
        /// and the  and the subsequent call to taos_subscribe can continue
        /// based on this progress; otherwise, the progress information will
        /// be deleted and the data can only be read again.
        ///  </param>
        [DllImport("taos", EntryPoint = "taos_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
        static extern private void Unsubscribe(IntPtr subscribe, int keep);

        /// <summary>
        /// Unsubscribe.
        /// </summary>
        /// <param name="subscribe"> Subscription object return by "Subscribe" </param>
        /// <param name="keep"> If it is not true, the API will keep the progress of subscription,
        /// and the  and the subsequent call to taos_subscribe can continue
        /// based on this progress; otherwise, the progress information will
        /// be deleted and the data can only be read again.
        ///  </param>
        /// <exception cref="Exception"></exception>
        static public void Unsubscribe(IntPtr subscribe, bool keep)
        {
            if (subscribe == IntPtr.Zero)
            {
                Console.WriteLine("subscribe is null, close Unsubscribe failed");
                throw new Exception("Object subscribe is null");
            }
            else
            {
                Unsubscribe(subscribe, keep == true ? 1 : 0);
                Console.WriteLine("Unsubscribe success.");
            }

        }
    }
}

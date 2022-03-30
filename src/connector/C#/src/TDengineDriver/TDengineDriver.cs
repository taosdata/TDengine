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

namespace TDengineDriver
{
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
        TSDB_DATA_TYPE_NCHAR = 10,   // unicode string
        TSDB_DATA_TYPE_UTINYINT = 11,// 1 byte
        TSDB_DATA_TYPE_USMALLINT = 12,// 2 bytes
        TSDB_DATA_TYPE_UINT = 13,    // 4 bytes
        TSDB_DATA_TYPE_UBIGINT = 14,   // 8 bytes
        TSDB_DATA_TYPE_JSONTAG = 15   //4096 bytes 
    }

    public enum TDengineInitOption
    {
        TSDB_OPTION_LOCALE = 0,
        TSDB_OPTION_CHARSET = 1,
        TSDB_OPTION_TIMEZONE = 2,
        TSDB_OPTION_CONFIGDIR = 3,
        TSDB_OPTION_SHELL_ACTIVITY_TIMER = 4
    }
    enum TaosField
    {
        STRUCT_SIZE = 68,
        NAME_LENGTH = 65,
        TYPE_OFFSET = 65,
        BYTES_OFFSET = 66,

    }
    public class TDengineMeta
    {
        public string name;
        public short size;
        public byte type;
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
                    return "undefine";
            }
        }
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public struct TAOS_BIND
    {
        // column type
        public int buffer_type;
        // one column value           
        public IntPtr buffer;
        // unused               
        public Int32 buffer_length;
        // actual value length in buffer   
        public IntPtr length;
        // indicates the column value is null or not      
        public IntPtr is_null;
        // unused            
        public int is_unsigned;
        // unused           
        public IntPtr error;
        public Int64 u;
        public uint allocated;
    }


    [StructLayout(LayoutKind.Sequential)]
    public struct TAOS_MULTI_BIND
    {
        // column type
        public int buffer_type;

        // array, one or more lines column value
        public IntPtr buffer;

        //length of element in TAOS_MULTI_BIND.buffer (for binary and nchar it is the longest element's length)
        public ulong buffer_length;

        //array, actual data length for each value
        public IntPtr length;

        //array, indicates each column value is null or not
        public IntPtr is_null;

        // line number, or the values number in buffer 
        public int num;
    }

    /// <summary>
    /// User defined callback function for interface "QueryAsync()"
    /// ,actually is a delegate in .Net.
    /// This function aim to handle the taoRes which points to
    /// the caller method's sql resultset.
    /// </summary>
    /// <param name="param"> This parameter will sent by caller method (QueryAsync()).</param>
    /// <param name="taoRes"> This is the retrieved by caller method's sql.</param>
    /// <param name="code"> 0 for indicate operation success and negative for operation fail.</param>
    public delegate void QueryAsyncCallback(IntPtr param, IntPtr taoRes, int code);

    /// <summary>
    /// User defined callback function for interface "FetchRowAsync()"
    /// ,actually is a delegate in .Net.
    /// This callback allow applications to get each row of the
    /// batch records by calling FetchRowAsync() forward iteration.
    /// After reading all the records in a block, the application needs to continue calling 
    /// FetchRowAsync() in this callback function to obtain the next batch of records for 
    /// processing until the number of records
    /// </summary>
    /// <param name="param">The parameter passed by <see cref="FetchRowAsync"/></param>
    /// <param name="taoRes">Query status</param>
    /// <param name="numOfRows"> The number of rows of data obtained (not a function of
    /// the entire query result set). When the number is zero (the result is returned) 
    /// or the number of records is negative (the query fails).</param>
    public delegate void FetchRowAsyncCallback(IntPtr param, IntPtr taoRes, int numOfRows);

    /// <summary>
    /// In asynchronous mode, the prototype of the callback function.
    /// </summary>
    /// <param name="subscribe">Subscription object return by <see cref = "Subscribe"> </param>
    /// <param name="tasRes"> Query retrieve result set. (Note there may be no record in the result set.)</param>
    /// <param name="param"> Additional parameters supplied by the client when taos_subscribe is called.</param>
    /// <param name="code"> Error code.</param>
    public delegate void SubscribeCallback(IntPtr subscribe, IntPtr tasRes, IntPtr param, int code);

    public class TDengine
    {
        public const int TSDB_CODE_SUCCESS = 0;

        [DllImport("taos", EntryPoint = "taos_init", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Init();

        [DllImport("taos", EntryPoint = "taos_cleanup", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Cleanup();

        [DllImport("taos", EntryPoint = "taos_options", CallingConvention = CallingConvention.Cdecl)]
        static extern public void Options(int option, string value);

        [DllImport("taos", EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr Connect(string ip, string user, string password, string db, short port);

        [DllImport("taos", EntryPoint = "taos_errstr", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr taos_errstr(IntPtr res);
        static public string Error(IntPtr res)
        {
            IntPtr errPtr = taos_errstr(res);
            return Marshal.PtrToStringAnsi(errPtr);
        }

        [DllImport("taos", EntryPoint = "taos_errno", CallingConvention = CallingConvention.Cdecl)]
        static extern public int ErrorNo(IntPtr res);

        [DllImport("taos", EntryPoint = "taos_query", CallingConvention = CallingConvention.Cdecl)]
        // static extern public IntPtr Query(IntPtr conn, string sqlstr);
        static extern private IntPtr Query(IntPtr conn, IntPtr byteArr);

        static public IntPtr Query(IntPtr conn, string command)
        {
            IntPtr res = IntPtr.Zero;

            IntPtr commandBuffer = Marshal.StringToCoTaskMemUTF8(command);
            res = Query(conn, commandBuffer);
            return res;
        }

        [DllImport("taos", EntryPoint = "taos_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        static extern public int AffectRows(IntPtr res);

        [DllImport("taos", EntryPoint = "taos_field_count", CallingConvention = CallingConvention.Cdecl)]
        static extern public int FieldCount(IntPtr res);

        [DllImport("taos", EntryPoint = "taos_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr taos_fetch_fields(IntPtr res);
        static public List<TDengineMeta> FetchFields(IntPtr res)
        {
            // const int fieldSize = 68;

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

        [DllImport("taos", EntryPoint = "taos_fetch_row", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr FetchRows(IntPtr res);

        [DllImport("taos", EntryPoint = "taos_free_result", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr FreeResult(IntPtr res);

        [DllImport("taos", EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
        static extern public int Close(IntPtr taos);

        //get precision of restultset
        [DllImport("taos", EntryPoint = "taos_result_precision", CallingConvention = CallingConvention.Cdecl)]
        static extern public int ResultPrecision(IntPtr taos);



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
        /// <param name="length">no used</param>
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

        [DllImport("taos", EntryPoint = "taos_load_table_info", CallingConvention = CallingConvention.Cdecl)]
        /// <summary>
        /// user application must call this API to  load all tables meta,
        /// </summary>
        /// <param name="taos">taos connection</param>
        /// <param name="tableList">tablelist</param>
        /// <returns></returns>
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
        /// which can only be used with QueryAsync().<c>FetchRowAsyncCallback<c>
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
        /// <param name="taos"> taos connection return by <see cref = "Connect"></param>
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
        /// <param name="subscribe"> Subscription object return by <see cref = "Subscribe">. </param>
        /// <returns></returns>
        [DllImport("taos", EntryPoint = "taos_consume", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr TaosConsume(IntPtr subscribe);
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
        /// <param name="subscribe"> Subscription object return by <see cref = "Subscribe">.</param>
        /// <param name="keep"> If it is not 0, the API will keep the progress of subscription,
        /// and the  and the subsequent call to taos_subscribe can continue
        /// based on this progress; otherwise, the progress information will
        /// be deleted and the data can only be read again.
        ///  </param>
        [DllImport("taos", EntryPoint = "taos_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
        static extern private void Unsubscribe(IntPtr subscribe, int keep);
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

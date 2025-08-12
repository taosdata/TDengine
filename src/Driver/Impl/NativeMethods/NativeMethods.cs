using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengineHelper;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static partial class NativeMethods
    {
        public const int TSDB_CODE_SUCCESS = 0;
        public const string DLLName = "taos";

        [DllImport(DLLName, EntryPoint = "taos_cleanup", CallingConvention = CallingConvention.Cdecl)]
        public static extern void Cleanup();

        [DllImport(DLLName, EntryPoint = "taos_options", CallingConvention = CallingConvention.Cdecl)]
        public static extern void Options(int option, string value);

        [DllImport(DLLName, EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr Connect(IntPtr ip, string user, string password, string db, ushort port);

        public static IntPtr Connect(string ip, string user, string password, string db, ushort port)
        {
            if (string.IsNullOrEmpty(ip))
            {
                return Connect(IntPtr.Zero, user, password, db, port);
            }
            else
            {
                UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(ip);
                var conn = Connect(utf8PtrStruct.utf8Ptr, user, password, db, port);
                utf8PtrStruct.UTF8FreePtr();
                return conn;
            }
        }

        [DllImport(DLLName, EntryPoint = "taos_errstr", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_errstr(IntPtr res);

        public static string Error(IntPtr res)
        {
            IntPtr errPtr = taos_errstr(res);
            return StringHelper.PtrToStringUTF8(errPtr);
        }

        [DllImport(DLLName, EntryPoint = "taos_errno", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ErrorNo(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_query", CallingConvention = CallingConvention.Cdecl)]
        // static extern public IntPtr Query(IntPtr conn, string sqlstr);
        private static extern IntPtr Query(IntPtr conn, IntPtr byteArr);

        public static IntPtr Query(IntPtr conn, string command)
        {
            IntPtr res = IntPtr.Zero;
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(command);
            res = Query(conn, utf8PtrStruct.utf8Ptr);
            utf8PtrStruct.UTF8FreePtr();
            return res;
        }

        // TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId);
        [DllImport(DLLName, EntryPoint = "taos_query_with_reqid", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr QueryWithReqid(IntPtr conn, IntPtr byteArr, long reqid);

        public static IntPtr QueryWithReqid(IntPtr conn, string command, long reqid)
        {
            IntPtr res = IntPtr.Zero;
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(command);
            res = QueryWithReqid(conn, utf8PtrStruct.utf8Ptr, reqid);
            utf8PtrStruct.UTF8FreePtr();
            return res;
        }

        [DllImport(DLLName, EntryPoint = "taos_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        public static extern int AffectRows(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_field_count", CallingConvention = CallingConvention.Cdecl)]
        public static extern int FieldCount(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_fetch_fields(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_fetch_fields_e", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_fetch_fields_e(IntPtr res);

        private static List<TDengineMeta> fetch_fields(IntPtr res, int fieldCount)
        {
            List<TDengineMeta> metaList = new List<TDengineMeta>();
            IntPtr fieldsPtr = taos_fetch_fields(res);
            for (int i = 0; i < fieldCount; ++i)
            {
                int offset = i * (int)TaosField.STRUCT_SIZE;
                TDengineMeta meta = new TDengineMeta();

                meta.name = StringHelper.PtrToStringUTF8(fieldsPtr + offset);
                meta.type = Marshal.ReadByte(fieldsPtr + offset + (int)TaosField.TYPE_OFFSET);
                meta.size = Marshal.ReadInt32(fieldsPtr + offset + (int)TaosField.BYTES_OFFSET);

                metaList.Add(meta);
            }

            return metaList;
        }

        private static List<TDengineMeta> fetch_fields_e(IntPtr res, int fieldCount)
        {
            List<TDengineMeta> metaList = new List<TDengineMeta>();
            IntPtr fieldsPtr = taos_fetch_fields_e(res);
            for (int i = 0; i < fieldCount; ++i)
            {
                TDengineMeta meta = new TDengineMeta();
                IntPtr fieldPtr = IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldE)));
                var field = (TaosFieldE)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldE));
                meta.name = field.name;
                meta.type = (byte)field.type;
                meta.size = field.bytes;
                meta.precision = field.precision;
                meta.scale = field.scale;
                metaList.Add(meta);
            }

            return metaList;
        }

        public static List<TDengineMeta> FetchFields(IntPtr res)
        {
            List<TDengineMeta> metaList = new List<TDengineMeta>();
            if (res == IntPtr.Zero)
            {
                return metaList;
            }

            int fieldCount = FieldCount(res);
            try
            {
                return fetch_fields_e(res, fieldCount);
            }
            catch (EntryPointNotFoundException)
            {
                return fetch_fields(res, fieldCount);
            }
        }

        [DllImport(DLLName, EntryPoint = "taos_fetch_row", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr FetchRows(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_free_result", CallingConvention = CallingConvention.Cdecl)]
        public static extern void FreeResult(IntPtr res);

        [DllImport(DLLName, EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern void Close(IntPtr taos);

        //get precision of restultset
        [DllImport(DLLName, EntryPoint = "taos_result_precision", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ResultPrecision(IntPtr taos);

        /// <summary>
        /// user application must call this API to  load all tables meta,
        /// </summary>
        /// <param name=DLLName>taos connection</param>
        /// <param name="tableList">table name List</param>
        /// <returns></returns>
        [DllImport(DLLName, EntryPoint = "taos_load_table_info", CallingConvention = CallingConvention.Cdecl)]
        private static extern int LoadTableInfoDll(IntPtr taos, string tableList);

        /// <summary>
        /// user application  call this API to load all tables meta,this method call the native
        /// method LoadTableInfoDll.
        /// this method must be called before StmtSetSubTbname(IntPtr stmt, string name);
        /// </summary>
        /// <param name=DLLName>taos connection</param>
        /// <param name="tableList">tables need to load meta info are form in an array</param>
        /// <returns></returns>
        public static int LoadTableInfo(IntPtr taos, string[] tableList)
        {
            string listStr = string.Join(",", tableList);
            return LoadTableInfoDll(taos, listStr);
        }

        [DllImport(DLLName, EntryPoint = "taos_fetch_lengths", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr FetchLengths(IntPtr taos);


        // ========================== Async Query====================
        /// <summary>
        /// This API uses non-blocking call mode.
        /// Application can open multiple tables and manipulate(query or insert) opened table concurrently.
        /// So applications must ensure that opetations on the same table is completely serialized.
        /// Because that will cause some query and insert operations cannot be performed.
        /// </summary>
        /// <param name=DLLName> A taos connection return by Connect()</param>
        /// <param name="sql">sql command need to execute</param>
        /// <param name="fq">User-defined callback function. <see cref="QueryAsyncCallback"/></param>
        /// <param name="param">the parameter for callback</param>
        [DllImport(DLLName, EntryPoint = "taos_query_a", CallingConvention = CallingConvention.Cdecl)]
        private static extern void _QueryAsync(IntPtr taos, IntPtr sql, QueryAsyncCallback fq, IntPtr param);

        public static void QueryAsync(IntPtr taos, string sql, QueryAsyncCallback fq, IntPtr param)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(sql);
            _QueryAsync(taos, utf8PtrStruct.utf8Ptr, fq, param);
            utf8PtrStruct.UTF8FreePtr();
        }

        [DllImport(DLLName, EntryPoint = "taos_query_a_with_reqid", CallingConvention = CallingConvention.Cdecl)]
        private static extern void _QueryAsyncWithReqid(IntPtr taos, IntPtr sql, QueryAsyncCallback fq, IntPtr param,
            long reqid);

        public static void QueryAsyncWithReqid(IntPtr taos, string sql, QueryAsyncCallback fq, IntPtr param, long reqid)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(sql);
            _QueryAsyncWithReqid(taos, utf8PtrStruct.utf8Ptr, fq, param, reqid);
            utf8PtrStruct.UTF8FreePtr();
        }

        // rawblock way
        [DllImport(DLLName, EntryPoint = "taos_fetch_raw_block_a", CallingConvention = CallingConvention.Cdecl)]
        public static extern void FetchRawBlockAsync(IntPtr taoRes, FetchRawBlockAsyncCallback fq, IntPtr param);
        //void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);

        [DllImport(DLLName, EntryPoint = "taos_get_raw_block", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr GetRawBlock(IntPtr taoRes);
        //const void* taos_get_raw_block(TAOS_RES * res);

        // Using TMQ in TDengine 3.0 to  instead Subscribe,Consume,Unsubscribe

        // ================================add =========================
        //int taos_select_db(TAOS *taos, const char *db);
        [DllImport(DLLName, EntryPoint = "taos_select_db", CallingConvention = CallingConvention.Cdecl)]
        private static extern int _SelectDatabase(IntPtr taos, IntPtr database);

        public static int SelectDatabase(IntPtr taos, string database)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(database);
            try
            {
                return _SelectDatabase(taos, utf8PtrStruct.utf8Ptr);
            }
            finally
            {
                utf8PtrStruct.UTF8FreePtr();
            }
        }

        //int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);

        //void  taos_stop_query(TAOS_RES *res);
        [DllImport(DLLName, EntryPoint = "taos_stop_query", CallingConvention = CallingConvention.Cdecl)]
        public static extern void StopQuery(IntPtr res);

        //bool taos_is_update_query(TAOS_RES *res);
        [DllImport(DLLName, EntryPoint = "taos_is_update_query", CallingConvention = CallingConvention.Cdecl)]
        public static extern byte IsUpdateQuery(IntPtr res);

        //int taos_validate_sql(TAOS *taos, const char *sql);
        [DllImport(DLLName, EntryPoint = "taos_validate_sql", CallingConvention = CallingConvention.Cdecl)]
        private static extern int _ValidateSQL(IntPtr taos, IntPtr sql);

        public static int ValidateSQL(IntPtr taos, string sql)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(sql);
            int code = _ValidateSQL(taos, utf8PtrStruct.utf8Ptr);
            utf8PtrStruct.UTF8FreePtr();
            return code;
        }

        // void taos_reset_current_db(TAOS *taos);
        [DllImport(DLLName, EntryPoint = "taos_reset_current_db", CallingConvention = CallingConvention.Cdecl)]
        public static extern void ResetCurrentDatabase(IntPtr taos);

        // TAOS_ROW *taos_result_block(TAOS_RES *res);

        // char *taos_get_server_info(TAOS *taos);
        [DllImport(DLLName, EntryPoint = "taos_get_server_info", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr _GetServerInfo(IntPtr taos);

        public static string GetServerInfo(IntPtr taos)
        {
            IntPtr serverInfoPtr = _GetServerInfo(taos);
            return StringHelper.PtrToStringUTF8(serverInfoPtr);
        }

        // char *taos_get_client_info();
        [DllImport(DLLName, EntryPoint = "taos_get_client_info", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr _GetClientInfo();

        public static string GetClientInfo()
        {
            IntPtr clientInfoPtr = _GetClientInfo();
            return StringHelper.PtrToStringUTF8(clientInfoPtr);
        }

        // ====================== 3.0 =====================
        //bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col);
        [DllImport(DLLName, EntryPoint = "taos_is_null", CallingConvention = CallingConvention.Cdecl)]
        public static extern bool IsNull(IntPtr res, Int32 row, Int32 col);

        //int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
        [DllImport(DLLName, EntryPoint = "taos_fetch_block", CallingConvention = CallingConvention.Cdecl)]
        public static extern int FetchBlock(IntPtr res, IntPtr rows);

        //int taos_fetch_raw_block(TAOS_RES *res, int* numOfRows, void** pData);
        [DllImport(DLLName, EntryPoint = "taos_fetch_raw_block", CallingConvention = CallingConvention.Cdecl)]
        public static extern int FetchRawBlock(IntPtr res, IntPtr numOfRows, IntPtr pData);

        //int* taos_get_column_data_offset(TAOS_RES *res, int columnIndex);
        [DllImport(DLLName, EntryPoint = "taos_get_column_data_offset", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr GetColDataOffset(IntPtr res, int columnIndex);

        // TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);
        [DllImport(DLLName, EntryPoint = "taos_check_server_status", CallingConvention = CallingConvention.Cdecl)]
        private static extern int _CheckServerStatus(IntPtr fqdn, int port, string detail, int maxlength);

        public static int CheckServerStatus(string fqdn, int port, string detail, int maxlength)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(fqdn);
            try
            {
                return _CheckServerStatus(utf8PtrStruct.utf8Ptr, port, detail, maxlength);
            }
            finally
            {
                utf8PtrStruct.UTF8FreePtr();
            }
        }
        // DLL_EXPORT int   taos_options_connection(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...);
        [DllImport(DLLName, EntryPoint = "taos_options_connection", CallingConvention = CallingConvention.Cdecl)]
        private static extern int OptionsConnection(IntPtr res, int option, IntPtr arg);

        public static int OptionsConnection(IntPtr res, int option, string arg)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(arg);
            try
            {
                return OptionsConnection(res, option, utf8PtrStruct.utf8Ptr);
            }
            finally
            {
                utf8PtrStruct.UTF8FreePtr();
            }
        }
        
        public static int CleanOptionsConnection(IntPtr res, int option)
        {
            // This is a workaround for the taos_options_connection function
            // that does not support cleaning options.
            return OptionsConnection(res, option, IntPtr.Zero);
        }
    }
}
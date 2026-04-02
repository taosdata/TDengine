using System;
using System.Runtime.InteropServices;
using TDengineHelper;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static partial class NativeMethods
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void TaosAsyncFnDelegate(IntPtr stmt, IntPtr res, int code, IntPtr userdata);

        [StructLayout(LayoutKind.Sequential)]
        public struct TAOS_STMT2_OPTION
        {
            public long reqid;
            [MarshalAs(UnmanagedType.I1)] public bool singleStbInsert;
            [MarshalAs(UnmanagedType.I1)] public bool singleTableBindOnce;
            public TaosAsyncFnDelegate asyncExecFn;
            public IntPtr userdata;
        }

        [DllImport(DLLName, EntryPoint = "taos_stmt2_init", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_stmt2_init(IntPtr taos, ref TAOS_STMT2_OPTION option);

        public static IntPtr TaosStmt2Init(IntPtr taos, long reqId, bool singleStbInsert, bool singleTableBindOnce)
        {
            var option = new TAOS_STMT2_OPTION
            {
                reqid = reqId,
                singleStbInsert = singleStbInsert,
                singleTableBindOnce = singleTableBindOnce,
                asyncExecFn = null,
                userdata = IntPtr.Zero
            };
            return taos_stmt2_init(taos, ref option);
        }

        // DLL_EXPORT int         taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_prepare", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_prepare(IntPtr stmt, IntPtr sql, ulong length);

        public static int TaosStmt2Prepare(IntPtr stmt, string sql)
        {
            UTF8PtrStruct sqlP = new UTF8PtrStruct(sql);
            int code = taos_stmt2_prepare(stmt, sqlP.utf8Ptr, (ulong)sqlP.utf8StrLength);
            sqlP.UTF8FreePtr();
            return code;
        }

        // DLL_EXPORT int         taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_bind_param", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_bind_param(IntPtr stmt, ref TAOS_STMT2_BINDV bindv, int col_idx);

        public static int TaosStmt2BindParam(IntPtr stmt, ref TAOS_STMT2_BINDV bindv)
        {
            return taos_stmt2_bind_param(stmt, ref bindv, -1);
        }

        // DLL_EXPORT int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_exec", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_exec(IntPtr stmt, out int affectedRows);
        
        public static int TaosStmt2Exec(IntPtr stmt, out int affectedRows)
        {
            return taos_stmt2_exec(stmt, out affectedRows);
        }

        // DLL_EXPORT int taos_stmt2_close(TAOS_STMT2 *stmt);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_close", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_close(IntPtr stmt);

        public static int TaosStmt2Close(IntPtr stmt)
        {
            return taos_stmt2_close(stmt);
        }

        // DLL_EXPORT int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_is_insert", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_is_insert(IntPtr stmt, out int isInsert);
        
        public static int TaosStmt2IsInsert(IntPtr stmt, out bool isInsert)
        {
            int code = taos_stmt2_is_insert(stmt, out int insert);
            isInsert = insert != 0;
            return code;
        }

        // DLL_EXPORT int taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_get_fields", CallingConvention = CallingConvention.Cdecl)]
        private static extern int taos_stmt2_get_fields(IntPtr stmt, out int count, out IntPtr fields);
        
        public static int TaosStmt2GetFields(IntPtr stmt,out int fieldsCount, out TaosFieldAll[] fields)
        {
            var code = taos_stmt2_get_fields(stmt, out var count, out var fieldsPtr);
            if (code != 0)
            {
                fieldsCount = 0;
                fields = null;
                return code;
            }

            fieldsCount = count;
            if (fieldsPtr == IntPtr.Zero)
            {
                fields = null;
                return 0;
            }
            
            fields = new TaosFieldAll[count];
            for (var i = 0; i < count; i++)
            {
                var fieldPtr =IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldAll)));
                fields[i] = (TaosFieldAll)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldAll));
            }
            taos_stmt2_free_fields(stmt,fieldsPtr);
            return 0;
        }

        // DLL_EXPORT void      taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_free_fields", CallingConvention = CallingConvention.Cdecl)]
        private static extern void taos_stmt2_free_fields(IntPtr stmt, IntPtr fields);

        // DLL_EXPORT TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_result", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_stmt2_result(IntPtr stmt);
        
        public static IntPtr TaosStmt2Result(IntPtr stmt)
        {
            return taos_stmt2_result(stmt);
        }

        // DLL_EXPORT char     *taos_stmt2_error(TAOS_STMT2 *stmt);
        [DllImport(DLLName, EntryPoint = "taos_stmt2_error", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr taos_stmt2_error(IntPtr stmt);

        public static string TaosStmt2Error(IntPtr stmt)
        {
            IntPtr errorPtr = taos_stmt2_error(stmt);
            return StringHelper.PtrToStringUTF8(errorPtr);
        }
    }
}
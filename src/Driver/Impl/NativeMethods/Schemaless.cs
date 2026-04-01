using System;
using System.Runtime.InteropServices;
using System.Text;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static partial class NativeMethods
    {
        //================================ schemaless ====================
        [DllImport(DLLName, SetLastError = true, EntryPoint = "taos_schemaless_insert",
            CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr SchemalessInsert(IntPtr taos, string[] lines, int numLines, int protocol,
            int precision);

        [DllImport(DLLName, SetLastError = true, EntryPoint = "taos_schemaless_insert_raw",
            CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr _taos_schemaless_insert_raw(IntPtr taos, byte[] lines, int length,
            IntPtr totalRows, int protocol, int precision);

        /// <summary>
        /// New schemaless insert interface,INFLUX_LINE_PROTOCOL AND OPTS_TELNET_PROTOCOL support '\0'(ASCII code 0) other is same as "TDengine.SchemalessInsert"
        /// </summary>
        /// <param name="taos">valid taos connect.</param>
        /// <param name="lines">Data want to insert.</param>
        /// <param name="protocol">Only INFLUX_LINE_PROTOCOL AND OPTS_TELNET_PROTOCOL support '\0'(ASCII code 0) input</param>
        /// <param name="precision"></param>
        /// <returns>Return number of rows have been inserted </returns>
        /// <exception cref="Exception"></exception>
        public static Int32 SchemalessInsertRaw(IntPtr taos, string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision)
        {
            IntPtr totalRowsPtr = Marshal.AllocHGlobal(sizeof(Int32));
            byte[] utf8Bytes = Encoding.UTF8.GetBytes(string.Join("\n", lines));


            //for (int i = 0; i < utf8Bytes.Length; i++)
            //{
            //    Console.WriteLine("i:{0}, {1}, ACSII:{2}", i, utf8Bytes[i], (int)utf8Bytes[i]);
            //}
            try
            {
                IntPtr smlRes = _taos_schemaless_insert_raw(taos, utf8Bytes, utf8Bytes.Length, totalRowsPtr,
                    (int)protocol, (int)precision);
                if (ErrorNo(smlRes) != 0)
                {
                    throw new Exception($"{Error(smlRes)},{ErrorNo(smlRes)}");
                }

                return Marshal.ReadInt32(totalRowsPtr);
            }
            finally
            {
                Marshal.FreeHGlobal(totalRowsPtr);
            }
        }

        // DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows,
        // int protocol, int precision, int32_t ttl, int64_t reqid);
        [DllImport(DLLName, EntryPoint = "taos_schemaless_insert_raw_ttl_with_reqid",
            CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr SchemalessInsertRawTTLWithReqid(
            IntPtr taos,
            byte[] lines,
            int len,
            out int totalRows,
            int protocol,
            int precision,
            int ttl,
            long reqid);
    }
}
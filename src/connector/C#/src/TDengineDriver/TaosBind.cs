using System;
using System.Runtime.InteropServices;
using System.Text;

namespace TDengineDriver
{
    /// <summary>
    /// this class used to get an instance of struct of TAO_BIND or TAOS_MULTI_BIND
    /// And the instance is corresponding with TDengine data type. For example, calling 
    /// "bindBinary"  will return a TAOS_BIND object that is corresponding with TDengine's
    /// binary type.
    /// </summary>
    public class TaosBind
    {
        public static TAOS_BIND BindBool(bool val)
        {
            TAOS_BIND bind = new TAOS_BIND();
            byte[] boolByteArr = BitConverter.GetBytes(val);
            int boolByteArrSize = Marshal.SizeOf(boolByteArr[0]) * boolByteArr.Length;
            IntPtr bo = Marshal.AllocHGlobal(1);
            Marshal.Copy(boolByteArr, 0, bo, boolByteArr.Length);

            int length = sizeof(Boolean);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
            bind.buffer = bo;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }
        public static TAOS_BIND BindTinyInt(sbyte val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] tinyIntByteArr = BitConverter.GetBytes(val);
            int tinyIntByteArrSize = Marshal.SizeOf(tinyIntByteArr[0]) * tinyIntByteArr.Length;
            IntPtr unmanagedTinyInt = Marshal.AllocHGlobal(tinyIntByteArrSize);
            Marshal.Copy(tinyIntByteArr, 0, unmanagedTinyInt, tinyIntByteArr.Length);

            int length = sizeof(sbyte);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
            bind.buffer = unmanagedTinyInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;
            return bind;

        }

        public static TAOS_BIND BindSmallInt(short val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr unmanagedSmallInt = Marshal.AllocHGlobal(sizeof(short));
            Marshal.WriteInt16(unmanagedSmallInt, val);

            int length = sizeof(short);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
            bind.buffer = unmanagedSmallInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindInt(int val)
        {
            TAOS_BIND bind = new TAOS_BIND();
            IntPtr unmanagedInt = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(unmanagedInt, val);

            int length = sizeof(int);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
            bind.buffer = unmanagedInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindBigInt(long val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr unmanagedBigInt = Marshal.AllocHGlobal(sizeof(long));
            Marshal.WriteInt64(unmanagedBigInt, val);

            int length = sizeof(long);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
            bind.buffer = unmanagedBigInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUTinyInt(byte val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            IntPtr unmanagedTinyInt = Marshal.AllocHGlobal(sizeof(byte));
            Marshal.WriteByte(unmanagedTinyInt, val);

            int length = sizeof(byte);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
            bind.buffer = unmanagedTinyInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUSmallInt(UInt16 val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] uSmallIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(uSmallIntByteArr[0]) * uSmallIntByteArr.Length;
            IntPtr unmanagedUnsignedSmallInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(uSmallIntByteArr, 0, unmanagedUnsignedSmallInt, uSmallIntByteArr.Length);

            int length = sizeof(UInt16);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
            bind.buffer = unmanagedUnsignedSmallInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUInt(uint val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] unmanagedIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(unmanagedIntByteArr[0]) * unmanagedIntByteArr.Length;
            IntPtr unmanagedInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(unmanagedIntByteArr, 0, unmanagedInt, unmanagedIntByteArr.Length);

            int length = sizeof(uint);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
            bind.buffer = unmanagedInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUBigInt(ulong val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] unmanagedBigIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(unmanagedBigIntByteArr[0]) * unmanagedBigIntByteArr.Length;
            IntPtr unmanagedBigInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(unmanagedBigIntByteArr, 0, unmanagedBigInt, unmanagedBigIntByteArr.Length);

            int length = sizeof(ulong);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
            bind.buffer = unmanagedBigInt;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindFloat(float val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] floatByteArr = BitConverter.GetBytes(val);
            int floatByteArrSize = Marshal.SizeOf(floatByteArr[0]) * floatByteArr.Length;
            IntPtr unmanagedFloat = Marshal.AllocHGlobal(floatByteArrSize);
            Marshal.Copy(floatByteArr, 0, unmanagedFloat, floatByteArr.Length);

            int length = sizeof(float);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
            bind.buffer = unmanagedFloat;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindDouble(Double val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] doubleByteArr = BitConverter.GetBytes(val);
            int doubleByteArrSize = Marshal.SizeOf(doubleByteArr[0]) * doubleByteArr.Length;
            IntPtr unmanagedDouble = Marshal.AllocHGlobal(doubleByteArrSize);
            Marshal.Copy(doubleByteArr, 0, unmanagedDouble, doubleByteArr.Length);

            int length = sizeof(Double);
            IntPtr lengthPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
            bind.buffer = unmanagedDouble;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindBinary(String val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            // IntPtr unmanagedBinary = Marshal.StringToHGlobalAnsi(val);
            IntPtr unmanagedBinary = Marshal.StringToCoTaskMemUTF8(val);

            var strToBytes = System.Text.Encoding.UTF8.GetBytes(val);
            int length = strToBytes.Length;
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(ulong));
            Marshal.WriteInt64(lenPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
            bind.buffer = unmanagedBinary;
            bind.buffer_length = length;
            bind.length = lenPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }
        public static TAOS_BIND BindNchar(String val)
        {
            TAOS_BIND bind = new TAOS_BIND();
            var strToBytes = System.Text.Encoding.UTF8.GetBytes(val);
            // IntPtr unmanagedNchar = (IntPtr)Marshal.StringToHGlobalAnsi(val);
            IntPtr unmanagedNchar = (IntPtr)Marshal.StringToCoTaskMemUTF8(val);


            int length = strToBytes.Length;
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(ulong));
            Marshal.WriteInt64(lenPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR;
            bind.buffer = unmanagedNchar;
            bind.buffer_length = length;
            bind.length = lenPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindNil()
        {
            TAOS_BIND bind = new TAOS_BIND();

            int isNull = 1;//IntPtr.Size;
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lenPtr, isNull);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_NULL;
            bind.is_null = lenPtr;
            return bind;
        }

        public static TAOS_BIND BindTimestamp(long ts)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr unmanagedTs = Marshal.AllocHGlobal(sizeof(long));
            Marshal.WriteInt64(unmanagedTs, ts);

            int length = sizeof(long);
            IntPtr lengthPtr = Marshal.AllocHGlobal(4);
            Marshal.WriteInt32(lengthPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
            bind.buffer = unmanagedTs;
            bind.buffer_length = length;
            bind.length = lengthPtr;
            bind.is_null = IntPtr.Zero;

            return bind;

        }

        public static void FreeTaosBind(TAOS_BIND[] binds)
        {
            foreach (TAOS_BIND bind in binds)
            {
                Marshal.FreeHGlobal(bind.buffer);
                Marshal.FreeHGlobal(bind.length);
                if (bind.is_null != IntPtr.Zero)
                {
                    // Console.WriteLine(bind.is_null);
                    Marshal.FreeHGlobal(bind.is_null);
                }

            }
        }
    }

}
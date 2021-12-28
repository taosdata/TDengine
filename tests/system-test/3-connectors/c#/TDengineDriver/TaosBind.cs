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
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
            bind.buffer = bo;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }
        public static TAOS_BIND BindTinyInt(sbyte val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] tinyIntByteArr = BitConverter.GetBytes(val);
            int tinyIntByteArrSize = Marshal.SizeOf(tinyIntByteArr[0]) * tinyIntByteArr.Length;
            IntPtr uManageTinyInt = Marshal.AllocHGlobal(tinyIntByteArrSize);
            Marshal.Copy(tinyIntByteArr, 0, uManageTinyInt, tinyIntByteArr.Length);

            int length = sizeof(sbyte);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
            bind.buffer = uManageTinyInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;
            return bind;

        }

        public static TAOS_BIND BindSmallInt(short val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr uManageSmallInt = Marshal.AllocHGlobal(sizeof(short));
            Marshal.WriteInt16(uManageSmallInt, val);

            int length = sizeof(short);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
            bind.buffer = uManageSmallInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindInt(int val)
        {
            TAOS_BIND bind = new TAOS_BIND();
            IntPtr uManageInt = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(uManageInt, val);

            int length = sizeof(int);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
            bind.buffer = uManageInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindBigInt(long val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr uManageBigInt = Marshal.AllocHGlobal(sizeof(long));
            Marshal.WriteInt64(uManageBigInt, val);

            int length = sizeof(long);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
            bind.buffer = uManageBigInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUTinyInt(byte val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            IntPtr uManageTinyInt = Marshal.AllocHGlobal(sizeof(byte));
            Marshal.WriteByte(uManageTinyInt, val);

            int length = sizeof(byte);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
            bind.buffer = uManageTinyInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUSmallInt(UInt16 val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] uSmallIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(uSmallIntByteArr[0]) * uSmallIntByteArr.Length;
            IntPtr uManageUnsignSmallInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(uSmallIntByteArr, 0, uManageUnsignSmallInt, uSmallIntByteArr.Length);

            int length = sizeof(UInt16);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
            bind.buffer = uManageUnsignSmallInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUInt(uint val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] uManageIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(uManageIntByteArr[0]) * uManageIntByteArr.Length;
            IntPtr uManageInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(uManageIntByteArr, 0, uManageInt, uManageIntByteArr.Length);

            int length = sizeof(uint);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
            bind.buffer = uManageInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindUBigInt(ulong val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] uManageBigIntByteArr = BitConverter.GetBytes(val);
            int usmallSize = Marshal.SizeOf(uManageBigIntByteArr[0]) * uManageBigIntByteArr.Length;
            IntPtr uManageBigInt = Marshal.AllocHGlobal(usmallSize);
            Marshal.Copy(uManageBigIntByteArr, 0, uManageBigInt, uManageBigIntByteArr.Length);

            int length = sizeof(ulong);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
            bind.buffer = uManageBigInt;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindFloat(float val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] floatByteArr = BitConverter.GetBytes(val);
            int floatByteArrSize = Marshal.SizeOf(floatByteArr[0]) * floatByteArr.Length;
            IntPtr uManageFloat = Marshal.AllocHGlobal(floatByteArrSize);
            Marshal.Copy(floatByteArr, 0, uManageFloat, floatByteArr.Length);

            int length = sizeof(float);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
            bind.buffer = uManageFloat;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindDouble(Double val)
        {
            TAOS_BIND bind = new TAOS_BIND();

            byte[] doubleByteArr = BitConverter.GetBytes(val);
            int doubleByteArrSize = Marshal.SizeOf(doubleByteArr[0]) * doubleByteArr.Length;
            IntPtr uManageDouble = Marshal.AllocHGlobal(doubleByteArrSize);
            Marshal.Copy(doubleByteArr, 0, uManageDouble, doubleByteArr.Length);

            int length = sizeof(Double);
            IntPtr lengPtr = Marshal.AllocHGlobal(sizeof(int));
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
            bind.buffer = uManageDouble;
            bind.buffer_length = length;
            bind.length = lengPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }

        public static TAOS_BIND BindBinary(String val)
        {

            TAOS_BIND bind = new TAOS_BIND();
            IntPtr umanageBinary = Marshal.StringToHGlobalAnsi(val);

            var strToBytes = System.Text.Encoding.Default.GetBytes(val);
            int leng = strToBytes.Length;
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(ulong));
            Marshal.WriteInt64(lenPtr, leng);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
            bind.buffer = umanageBinary;
            bind.buffer_length = leng;
            bind.length = lenPtr;
            bind.is_null = IntPtr.Zero;

            return bind;
        }
        public static TAOS_BIND BindNchar(String val)
        {
            TAOS_BIND bind = new TAOS_BIND();
            var strToBytes = System.Text.Encoding.Default.GetBytes(val);
            IntPtr umanageNchar = (IntPtr)Marshal.StringToHGlobalAnsi(val);


            int leng = strToBytes.Length;
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(ulong));
            Marshal.WriteInt64(lenPtr, leng);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR;
            bind.buffer = umanageNchar;
            bind.buffer_length = leng;
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
            IntPtr uManageTs = Marshal.AllocHGlobal(sizeof(long));
            Marshal.WriteInt64(uManageTs, ts);

            int length = sizeof(long);
            IntPtr lengPtr = Marshal.AllocHGlobal(4);
            Marshal.WriteInt32(lengPtr, length);

            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
            bind.buffer = uManageTs;
            bind.buffer_length = length;
            bind.length = lengPtr;
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
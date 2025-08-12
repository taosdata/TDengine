using System;
using System.Drawing;
using System.Runtime.InteropServices;
using System.Text;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static class MultiBind
    {
        private static TAOS_MULTI_BIND MultiBindT<T>(T?[] arr, TDengineDataType dataType)
            where T : struct
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = TDengineConstant.TypeLengthMap[dataType];

            IntPtr lengthArr = Marshal.AllocHGlobal(TDengineConstant.Int32Size * elementCount);
            IntPtr bufferPtr = Marshal.AllocHGlobal(typeSize * elementCount);
            IntPtr nullArr = Marshal.AllocHGlobal(TDengineConstant.ByteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                if (arr[i] != null)
                {
                    Marshal.StructureToPtr(arr[i], bufferPtr + typeSize * i, false);
                    Marshal.WriteInt32(lengthArr, TDengineConstant.Int32Size * i, typeSize);
                }

                Marshal.WriteByte(nullArr, TDengineConstant.ByteSize * i, arr[i] == null ? (byte)1 : (byte)0);
            }

            multiBind.buffer_type = (int)dataType;
            multiBind.buffer = bufferPtr;
            multiBind.buffer_length = (UIntPtr)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        private static TAOS_MULTI_BIND MultiBindT<T>(T[] arr, TDengineDataType dataType)
            where T : struct
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = TDengineConstant.TypeLengthMap[dataType];

            IntPtr lengthArr = Marshal.AllocHGlobal(TDengineConstant.Int32Size * elementCount);
            IntPtr bufferPtr = Marshal.AllocHGlobal(typeSize * elementCount);
            IntPtr nullArr = Marshal.AllocHGlobal(TDengineConstant.ByteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                Marshal.StructureToPtr(arr[i], bufferPtr + typeSize * i, false);
                Marshal.WriteByte(nullArr, TDengineConstant.ByteSize * i, 0);
                Marshal.WriteInt32(lengthArr, TDengineConstant.Int32Size * i, typeSize);
            }
            multiBind.buffer_type = (int)dataType;
            multiBind.buffer = bufferPtr;
            multiBind.buffer_length = (UIntPtr)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        public static TAOS_MULTI_BIND MultiBindBool(bool?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_BOOL);
        }

        public static TAOS_MULTI_BIND MultiBindBool(bool[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_BOOL);
        }

        public static TAOS_MULTI_BIND MultiBindTinyInt(sbyte?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_TINYINT);
        }

        public static TAOS_MULTI_BIND MultiBindTinyInt(sbyte[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_TINYINT);
        }

        public static TAOS_MULTI_BIND MultiBindSmallInt(short?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
        }

        public static TAOS_MULTI_BIND MultiBindSmallInt(short[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
        }

        public static TAOS_MULTI_BIND MultiBindInt(int?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_INT);
        }

        public static TAOS_MULTI_BIND MultiBindInt(int[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_INT);
        }

        public static TAOS_MULTI_BIND MultiBindBigInt(long?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_BIGINT);
        }

        public static TAOS_MULTI_BIND MultiBindBigInt(long[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_BIGINT);
        }

        public static TAOS_MULTI_BIND MultiBindFloat(float?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_FLOAT);
        }

        public static TAOS_MULTI_BIND MultiBindFloat(float[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_FLOAT);
        }

        public static TAOS_MULTI_BIND MultiBindDouble(double?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
        }

        public static TAOS_MULTI_BIND MultiBindDouble(double[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
        }

        public static TAOS_MULTI_BIND MultiBindUTinyInt(byte?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
        }

        public static TAOS_MULTI_BIND MultiBindUTinyInt(byte[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
        }


        public static TAOS_MULTI_BIND MultiBindUSmallInt(ushort?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
        }

        public static TAOS_MULTI_BIND MultiBindUSmallInt(ushort[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
        }

        public static TAOS_MULTI_BIND MultiBindUInt(uint?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UINT);
        }

        public static TAOS_MULTI_BIND MultiBindUInt(uint[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UINT);
        }

        public static TAOS_MULTI_BIND MultiBindUBigInt(ulong?[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
        }

        public static TAOS_MULTI_BIND MultiBindUBigInt(ulong[] arr)
        {
            return MultiBindT(arr, TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
        }

        public static TAOS_MULTI_BIND MultiBindStringArray(string[] arr, TDengineDataType fieldType)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = MaxElementLength(arr);

            IntPtr lengthArr = Marshal.AllocHGlobal(TDengineConstant.Int32Size * elementCount);
            IntPtr nullArr = Marshal.AllocHGlobal(TDengineConstant.ByteSize * elementCount);
            IntPtr bufferPtr = Marshal.AllocHGlobal(typeSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                if (!String.IsNullOrEmpty(arr[i]))
                {
                    byte[] decodeByte = GetStringEncodeByte(arr[i]);
                    itemLength = decodeByte.Length;
                    Marshal.Copy(decodeByte, 0, bufferPtr + i * typeSize, itemLength);
                }

                Marshal.WriteInt32(lengthArr, TDengineConstant.Int32Size * i, itemLength);
                Marshal.WriteByte(nullArr, TDengineConstant.ByteSize * i, arr[i] == null ? (byte)1 : (byte)0);
            }

            multiBind.buffer_type = (int)fieldType;
            multiBind.buffer = bufferPtr;
            multiBind.buffer_length = (UIntPtr)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        public static TAOS_MULTI_BIND MultiBindBytesArray(byte[][] arr, TDengineDataType bufferType)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = MaxElementLength(arr);

            IntPtr lengthArr = Marshal.AllocHGlobal(TDengineConstant.Int32Size * elementCount);
            IntPtr nullArr = Marshal.AllocHGlobal(TDengineConstant.ByteSize * elementCount);
            IntPtr bufferPtr = Marshal.AllocHGlobal(typeSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                if (arr[i] != null)
                {
                    byte[] decodeByte = arr[i];
                    itemLength = decodeByte.Length;
                    Marshal.Copy(arr[i], 0, bufferPtr + i * typeSize, arr[i].Length);
                }

                Marshal.WriteInt32(lengthArr, TDengineConstant.Int32Size * i, itemLength);
                Marshal.WriteByte(nullArr, TDengineConstant.ByteSize * i, arr[i] == null ? (byte)1 : (byte)0);
            }

            multiBind.buffer_type = (int)bufferType;
            multiBind.buffer = bufferPtr;
            multiBind.buffer_length = (UIntPtr)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        
        public static TAOS_MULTI_BIND MultiBindTimestamp(DateTime?[] arr, TDenginePrecision precision)
        {
            return MultiBindTimestampInternal(arr, (value) =>
            {
                if (value.HasValue)
                {
                    return TDengineConstant.ConvertDateTimeToTimestamp(value.Value,precision);
                }

                return 0;
            });
        }

        public static TAOS_MULTI_BIND MultiBindTimestamp(DateTime[] arr, TDenginePrecision precision)
        {
            return MultiBindTimestampInternal(arr,
                (value) => TDengineConstant.ConvertDateTimeToTimestamp(value,precision));
        }

        public static TAOS_MULTI_BIND MultiBindTimestamp(DateTimeOffset?[] arr, TDenginePrecision precision)
        {
            return MultiBindTimestampInternal(arr, (value) =>
            {
                if (value.HasValue)
                {
                    return TDengineConstant.ConvertDateTimeOffsetToTimestamp(value.Value,precision);
                }

                return 0;
            });
        }
        
        public static TAOS_MULTI_BIND MultiBindTimestamp(DateTimeOffset[] arr, TDenginePrecision precision)
        {
            return MultiBindTimestampInternal(arr,
                (value) => TDengineConstant.ConvertDateTimeOffsetToTimestamp(value,precision));
        }

        public static TAOS_MULTI_BIND MultiBindTimestamp(long?[] arr)
        {
            return MultiBindTimestampInternal(arr, (value) => value ?? 0);
        }

        public static TAOS_MULTI_BIND MultiBindTimestamp(long[] arr)
        {
            return MultiBindTimestampInternal(arr, (value) => value);
        }

        private static TAOS_MULTI_BIND MultiBindTimestampInternal<T>(T[] arr, Func<T, long> valueFunc)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = TDengineConstant.Int64Size;

            IntPtr bufferPtr = Marshal.AllocHGlobal(typeSize * elementCount);
            IntPtr nullArr = Marshal.AllocHGlobal(TDengineConstant.ByteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                long value = valueFunc(arr[i]);
                Marshal.WriteInt64(bufferPtr, typeSize * i, value);
                Marshal.WriteByte(nullArr, TDengineConstant.ByteSize * i, arr[i] == null ? (byte)1 : (byte)0);
            }

            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
            multiBind.buffer = bufferPtr;
            multiBind.buffer_length = (UIntPtr)typeSize;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        
        public static void FreeTaosBind(TAOS_MULTI_BIND bind)
        {
            if (bind.buffer != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(bind.buffer);
            }

            if (bind.length != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(bind.length);
            }

            if (bind.is_null != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(bind.is_null);
            }
        }

        private static int MaxElementLength(string[] strArr)
        {
            int max = 0;
            foreach (string str in strArr)
            {
                if (!String.IsNullOrEmpty(str))
                {
                    int tmpLength = Encoding.UTF8.GetByteCount(str);
                    if (max < tmpLength)
                    {
                        max = tmpLength;
                    }
                }
            }

            return max;
        }

        private static int MaxElementLength(byte[][] bytesArr)
        {
            int max = 0;
            for (int i = 0; i < bytesArr.Length; i++)
            {
                if (bytesArr[i] != null)
                {
                    int tmpLength = bytesArr[i].Length;
                    if (max < tmpLength)
                    {
                        max = tmpLength;
                    }
                }
            }

            return max;
        }

        private static Byte[] GetStringEncodeByte(string str)
        {
            if (str == null)
            {
                return null;
            }

            return str.Length == 0 ? new byte[0] : Encoding.UTF8.GetBytes(str);
        }
    }
}
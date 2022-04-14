using System;
using System.Text;
using System.Runtime.InteropServices;

namespace TDengineDriver
{
    public class TaosMultiBind
    {
        public static TAOS_MULTI_BIND MultiBindBool(bool?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            //the size of buffer array element
            int typeSize = sizeof(bool);
            //size of int 
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);

            //TAOS_MULTI_BIND.buffer
            IntPtr c_bool_ptr = Marshal.AllocHGlobal(elementCount * typeSize);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(c_bool_ptr, typeSize * i, Convert.ToByte(arr[i] ?? false));
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
            multiBind.buffer = c_bool_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindTinyInt(sbyte?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            //the size of buffer array element
            int typeSize = sizeof(byte);
            int byteSize = sizeof(byte);
            //size of int 
            int intSize = sizeof(int);

            //TAOS_MULTI_BIND.buffer
            IntPtr c_tinyint_ptr = Marshal.AllocHGlobal(elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(intSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                Byte[] toByteArr = BitConverter.GetBytes(arr[i] ?? sbyte.MinValue);

                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(c_tinyint_ptr, typeSize * i, toByteArr[0]);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
            multiBind.buffer = c_tinyint_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindSmallInt(short?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            //the size of buffer array element
            int typeSize = sizeof(short);
            //size of int 
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);

            //TAOS_MULTI_BIND.buffer
            IntPtr c_smallint_ptr = Marshal.AllocHGlobal(elementCount * typeSize);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt16(c_smallint_ptr, typeSize * i, arr[i] ?? short.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));

            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
            multiBind.buffer = c_smallint_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindInt(int?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(int);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);

            //TAOS_MULTI_BIND.buffer
            IntPtr c_int_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt32(c_int_ptr, typeSize * i, arr[i] ?? int.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));

            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
            multiBind.buffer = c_int_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindBigint(long?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(long);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);

            //TAOS_MULTI_BIND.buffer
            IntPtr c_long_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt64(c_long_ptr, typeSize * i, arr[i] ?? long.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));


            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
            multiBind.buffer = c_long_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindFloat(float?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(float);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 
            float[] arrTmp = new float[elementCount];

            //TAOS_MULTI_BIND.buffer
            IntPtr c_float_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                arrTmp[i] = arr[i] ?? float.MinValue;
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }
            //set TAOS_MULTI_BIND.buffer
            Marshal.Copy(arrTmp, 0, c_float_ptr, elementCount);

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
            multiBind.buffer = c_float_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindDouble(double?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(double);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 
            double[] arrTmp = new double[elementCount];

            //TAOS_MULTI_BIND.buffer
            IntPtr c_double_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                arrTmp[i] = arr[i] ?? double.MinValue;
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }
            //set TAOS_MULTI_BIND.buffer
            Marshal.Copy(arrTmp, 0, c_double_ptr, elementCount);

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
            multiBind.buffer = c_double_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindUTinyInt(byte?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(byte);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 

            //TAOS_MULTI_BIND.buffer
            IntPtr c_unsigned_tinyint_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(c_unsigned_tinyint_ptr, typeSize * i, arr[i] ?? byte.MaxValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
            multiBind.buffer = c_unsigned_tinyint_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindUSmallInt(ushort?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(ushort);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 

            //TAOS_MULTI_BIND.buffer
            IntPtr c_unsigned_smallint_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                byte[] byteArr = BitConverter.GetBytes(arr[i] ?? ushort.MaxValue);
                for (int j = 0; j < byteArr.Length; j++)
                {
                    //set TAOS_MULTI_BIND.buffer
                    Marshal.WriteByte(c_unsigned_smallint_ptr, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
            multiBind.buffer = c_unsigned_smallint_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindUInt(uint?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(uint);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 

            //TAOS_MULTI_BIND.buffer
            IntPtr c_unsigned_int_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                byte[] byteArr = BitConverter.GetBytes(arr[i] ?? uint.MaxValue);
                for (int j = 0; j < byteArr.Length; j++)
                {
                    //set TAOS_MULTI_BIND.buffer
                    Marshal.WriteByte(c_unsigned_int_ptr, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
            multiBind.buffer = c_unsigned_int_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindUBigInt(ulong?[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(ulong);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //used to replace null 

            //TAOS_MULTI_BIND.buffer
            IntPtr c_unsigned_bigint_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                byte[] byteArr = BitConverter.GetBytes(arr[i] ?? ulong.MaxValue);
                for (int j = 0; j < byteArr.Length; j++)
                {
                    //set TAOS_MULTI_BIND.buffer
                    Marshal.WriteByte(c_unsigned_bigint_ptr, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
            multiBind.buffer = c_unsigned_bigint_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }
        public static TAOS_MULTI_BIND MultiBindBinary(string[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            //TypeSize represent the Max element length of the comming arr
            //The size of the buffer is typeSize * elementCount
            //This buffer is used to store TAOS_MULTI_BIND.buffer
            int typeSize = MaxElementLength(arr);
            //This intSize is used to calculate buffer size of the struct TAOS_MULTI_BIND's 
            //length. The buffer is intSize * elementCount,which is used to store TAOS_MULTI_BIND.length
            int intSize = sizeof(int);
            //This byteSize is used to calculate the buffer size of the struct TAOS_MULTI_BIND.is_null
            //This buffer size is byteSize * elementCount
            int byteSize = sizeof(byte);

            StringBuilder arrStrBuilder = new StringBuilder(); ;
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);
            //TAOS_MULTI_BIND.buffer
            IntPtr c_char_prt = Marshal.AllocHGlobal(typeSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                byte[] decodeByte = GetStringEncodeByte(arr[i]);
                itemLength = decodeByte.Length;
                if (!String.IsNullOrEmpty(arr[i]))
                {
                    for (int j = 0; j < itemLength; j++)
                    {
                        //Read byte after byte
                        Marshal.WriteByte(c_char_prt, i * typeSize + j, decodeByte[j]);
                    }
                }
                //Set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, itemLength);
                //Set TAOS_MULTI_BIND.is_null 
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(String.IsNullOrEmpty(arr[i]) ? 1 : 0));
            }
            //Config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
            multiBind.buffer = c_char_prt;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        public static TAOS_MULTI_BIND MultiBindNchar(string[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            //TypeSize represent the Max element length of the comming arr
            //The size of the buffer is typeSize * elementCount
            //This buffer is used to store TAOS_MULTI_BIND.buffer
            int typeSize = MaxElementLength(arr);
            //This intSize is used to calculate buffer size of the struct TAOS_MULTI_BIND's 
            //length. The buffer is intSize * elementCount,which is used to store TAOS_MULTI_BIND.length
            int intSize = sizeof(int);
            //This byteSize is used to calculate the buffer size of the struct TAOS_MULTI_BIND.is_null
            //This buffer size is byteSize * elementCount
            int byteSize = sizeof(byte);

            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);
            //TAOS_MULTI_BIND.buffer
            IntPtr c_char_prt = Marshal.AllocHGlobal(typeSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                byte[] decodeByte = GetStringEncodeByte(arr[i]);
                itemLength = decodeByte.Length;
                if (!String.IsNullOrEmpty(arr[i]))
                {
                    for (int j = 0; j < itemLength; j++)
                    {
                        //Read byte after byte
                        Marshal.WriteByte(c_char_prt, i * typeSize + j, decodeByte[j]);
                    }
                }
                //Set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, itemLength);
                //Set TAOS_MULTI_BIND.is_null 
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(String.IsNullOrEmpty(arr[i]) ? 1 : 0));
            }
            //Config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR;
            multiBind.buffer = c_char_prt;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        public static TAOS_MULTI_BIND MultiBindTimestamp(long[] arr)
        {
            TAOS_MULTI_BIND multiBind = new TAOS_MULTI_BIND();
            int elementCount = arr.Length;
            int typeSize = sizeof(long);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            //TAOS_MULTI_BIND.buffer
            IntPtr c_long_ptr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt64(c_long_ptr, typeSize * i, arr[i]);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, 0);
            }

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
            multiBind.buffer = c_long_ptr;
            multiBind.buffer_length = (ulong)typeSize;
            multiBind.length = lengthArr;
            multiBind.is_null = nullArr;
            multiBind.num = elementCount;

            return multiBind;
        }

        public static void FreeTaosBind(TAOS_MULTI_BIND[] mBinds)
        {
            foreach (TAOS_MULTI_BIND bind in mBinds)
            {
                Marshal.FreeHGlobal(bind.buffer);
                Marshal.FreeHGlobal(bind.length);
                Marshal.FreeHGlobal(bind.is_null);
            }
        }

        private static char[] AlignCharArr(int offSet)
        {
            char[] alignChar = new char[offSet];
            for (int i = 0; i < offSet; i++)
            {
                alignChar[i] = char.MinValue;
            }
            return alignChar;
        }

        private static int MaxElementLength(String[] strArr)
        {
            int max = 0;
            for (int i = 0; i < strArr.Length; i++)
            {
                int tmpLength = GetStringEncodeByte(strArr[i]).Length;
                if (!String.IsNullOrEmpty(strArr[i]) && max < tmpLength)
                {
                    max = tmpLength;
                }
            }
            return max;
        }

        private static Byte[] GetStringEncodeByte(string str)
        {
            Byte[] strToBytes = null;
            if (String.IsNullOrEmpty(str))
            {
                strToBytes = System.Text.Encoding.Default.GetBytes(String.Empty);
            }
            else
            {
                strToBytes = System.Text.Encoding.Default.GetBytes(str);
            }
            return strToBytes;
        }
    }

}
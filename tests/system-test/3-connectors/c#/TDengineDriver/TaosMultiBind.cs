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
            IntPtr unmanagedBoolArr = Marshal.AllocHGlobal(elementCount * typeSize);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(unmanagedBoolArr, typeSize * i, Convert.ToByte(arr[i] ?? false));
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
            multiBind.buffer = unmanagedBoolArr;
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
            IntPtr unmanagedTintIntArr = Marshal.AllocHGlobal(elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(intSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                Byte[] toByteArr = BitConverter.GetBytes(arr[i] ?? sbyte.MinValue);

                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(unmanagedTintIntArr, typeSize * i, toByteArr[0]);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
            multiBind.buffer = unmanagedTintIntArr;
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
            IntPtr unmanagedSmallIntArr = Marshal.AllocHGlobal(elementCount * typeSize);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt16(unmanagedSmallIntArr, typeSize * i, arr[i] ?? short.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));

            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
            multiBind.buffer = unmanagedSmallIntArr;
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
            IntPtr intBuff = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt32(intBuff, typeSize * i, arr[i] ?? int.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));

            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
            multiBind.buffer = intBuff;
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
            IntPtr intBuff = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt64(intBuff, typeSize * i, arr[i] ?? long.MinValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));


            }
            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
            multiBind.buffer = intBuff;
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
            IntPtr floatBuff = Marshal.AllocHGlobal(typeSize * elementCount);
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
            Marshal.Copy(arrTmp, 0, floatBuff, elementCount);

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
            multiBind.buffer = floatBuff;
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
            IntPtr doubleBuff = Marshal.AllocHGlobal(typeSize * elementCount);
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
            Marshal.Copy(arrTmp, 0, doubleBuff, elementCount);

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
            multiBind.buffer = doubleBuff;
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
            IntPtr uTinyIntBuff = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);


            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteByte(uTinyIntBuff, typeSize * i, arr[i] ?? byte.MaxValue);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
            multiBind.buffer = uTinyIntBuff;
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
            IntPtr uSmallIntBuff = Marshal.AllocHGlobal(typeSize * elementCount);
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
                    Marshal.WriteByte(uSmallIntBuff, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
            multiBind.buffer = uSmallIntBuff;
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
            IntPtr uIntBuff = Marshal.AllocHGlobal(typeSize * elementCount);
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
                    Marshal.WriteByte(uIntBuff, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
            multiBind.buffer = uIntBuff;
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
            IntPtr uBigIntBuff = Marshal.AllocHGlobal(typeSize * elementCount);
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
                    Marshal.WriteByte(uBigIntBuff, typeSize * i + j * byteSize, byteArr[j]);
                }
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(arr[i].Equals(null) ? 1 : 0));
            }


            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
            multiBind.buffer = uBigIntBuff;
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
            int typeSize = MaxElementLength(arr);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            StringBuilder arrStrBuilder = new StringBuilder(); ;

            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                byte[] decodeByte = GetStringEncodeByte(arr[i]);
                itemLength = decodeByte.Length;
                // if element if not null and element length is less then typeSize
                // fill the memory with default char.Since arr element memory need align.
                if (!String.IsNullOrEmpty(arr[i]) && typeSize == itemLength)
                {
                    arrStrBuilder.Append(arr[i]);
                }
                else if (!String.IsNullOrEmpty(arr[i]) && typeSize > itemLength)
                {
                    arrStrBuilder.Append(arr[i]);
                    arrStrBuilder.Append(AlignCharArr(typeSize - itemLength));
                }
                else
                {
                    // if is null value,fill the memory with default values.
                    arrStrBuilder.Append(AlignCharArr(typeSize));
                }

                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null 
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(String.IsNullOrEmpty(arr[i]) ? 1 : 0));
            }
            //set TAOS_MULTI_BIND.buffer
            IntPtr uBinaryBuff = (IntPtr)Marshal.StringToHGlobalAnsi(arrStrBuilder.ToString());

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
            multiBind.buffer = uBinaryBuff;
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
            int typeSize = MaxElementLength(arr);
            int intSize = sizeof(int);
            int byteSize = sizeof(byte);
            StringBuilder arrStrBuilder = new StringBuilder(); ;

            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                int itemLength = 0;
                byte[] decodeByte = GetStringEncodeByte(arr[i]);
                itemLength = decodeByte.Length;
                // if element if not null and element length is less then typeSize
                // fill the memory with default char.Since arr element memory need align.
                if (!String.IsNullOrEmpty(arr[i]) && typeSize == itemLength)
                {
                    arrStrBuilder.Append(arr[i]);
                }
                else if (!String.IsNullOrEmpty(arr[i]) && typeSize > itemLength)
                {
                    arrStrBuilder.Append(arr[i]);
                    arrStrBuilder.Append(AlignCharArr(typeSize - itemLength));
                }
                else
                {
                    // if is null value,fill the memory with default values.
                    arrStrBuilder.Append(AlignCharArr(typeSize));
                }

                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null 
                Marshal.WriteByte(nullArr, byteSize * i, Convert.ToByte(String.IsNullOrEmpty(arr[i]) ? 1 : 0));
            }
            //set TAOS_MULTI_BIND.buffer
            IntPtr uNcharBuff = (IntPtr)Marshal.StringToHGlobalAnsi(arrStrBuilder.ToString());

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR;
            multiBind.buffer = uNcharBuff;
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
            IntPtr unmanagedTsArr = Marshal.AllocHGlobal(typeSize * elementCount);
            //TAOS_MULTI_BIND.length
            IntPtr lengthArr = Marshal.AllocHGlobal(intSize * elementCount);
            //TAOS_MULTI_BIND.is_null
            IntPtr nullArr = Marshal.AllocHGlobal(byteSize * elementCount);

            for (int i = 0; i < elementCount; i++)
            {
                //set TAOS_MULTI_BIND.buffer
                Marshal.WriteInt64(unmanagedTsArr, typeSize * i, arr[i]);
                //set TAOS_MULTI_BIND.length
                Marshal.WriteInt32(lengthArr, intSize * i, typeSize);
                //set TAOS_MULTI_BIND.is_null
                Marshal.WriteByte(nullArr, byteSize * i, 0);
            }

            //config TAOS_MULTI_BIND
            multiBind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
            multiBind.buffer = unmanagedTsArr;
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
            if(String.IsNullOrEmpty(str))
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
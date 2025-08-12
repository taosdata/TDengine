using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace TDengine.Driver
{
    public class TypeArrayMismatchException : Exception
    {
        public TypeArrayMismatchException() : base("Number of types does not match number of arrays.")
        {
        }
    }

    public class RowCountMismatchException : Exception
    {
        public RowCountMismatchException() : base("Number of rows in array does not match expected number.")
        {
        }
    }

    public static class BlockWriter
    {
        public static byte[] Serialize(int rows, TaosFieldE[] fields, params Array[] arrays)
        {
            if (fields.Length == 0)
            {
                return null;
            }

            if (fields.Length != arrays.Length)
            {
                throw new TypeArrayMismatchException();
            }

            foreach (var array in arrays)
            {
                if (array.Length != rows)
                {
                    throw new RowCountMismatchException();
                }
            }

            var columns = fields.Length;

            List<byte> block = new List<byte>();
            // version int32
            AppendUint32(block, 1);
            // length int32
            AppendUint32(block, 0);
            // rows int32
            AppendUint32(block, (uint)rows);
            // columns int32
            AppendUint32(block, (uint)arrays.Length);
            // flagSegment int32
            AppendUint32(block, 0);
            // groupID uint64
            AppendUint64(block, 0);

            var colInfoData = new List<byte>(5 * columns);
            var lengthData = new List<byte>(4 * columns);
            var bitMapLen = TDengineConstant.BitmapLen(rows);
            List<byte> data = new List<byte>();
            for (int colIndex = 0; colIndex < columns; colIndex++)
            {
                var array = arrays[colIndex];
                var elementType = array.GetType().GetElementType();
                if ((TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                {
                    if (elementType == typeof(long))
                    {
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long[])array,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else if (elementType == typeof(long?))
                    {
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long[])array,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else if (elementType == typeof(DateTime))
                    {
                        var vv = new long[rows];
                        var v = (DateTime[])array;
                        for (int i = 0; i < rows; i++)
                        {
                            vv[i] = TDengineConstant.ConvertDateTimeToTimestamp(v[i],
                                (TDenginePrecision)fields[colIndex].precision);
                        }

                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, vv,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else if (elementType == typeof(DateTime?))
                    {
                        var vv = new long?[rows];
                        var v = (DateTime?[])array;
                        for (int i = 0; i < rows; i++)
                        {
                            if (v[i] == null)
                            {
                                vv[i] = null;
                            }
                            else
                            {
                                vv[i] = TDengineConstant.ConvertDateTimeToTimestamp(v[i].Value,
                                    (TDenginePrecision)fields[colIndex].precision);
                            }
                        }

                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, vv,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else if (elementType == typeof(DateTimeOffset))
                    {
                        var vv = new long[rows];
                        var v = (DateTimeOffset[])array;
                        for (int i = 0; i < rows; i++)
                        {
                            vv[i] = TDengineConstant.ConvertDateTimeOffsetToTimestamp(v[i],
                                (TDenginePrecision)fields[colIndex].precision);
                        }

                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, vv,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else if (elementType == typeof(DateTimeOffset?))
                    {
                        var vv = new long?[rows];
                        var v = (DateTimeOffset?[])array;
                        for (int i = 0; i < rows; i++)
                        {
                            if (v[i] == null)
                            {
                                vv[i] = null;
                            }
                            else
                            {
                                vv[i] = TDengineConstant.ConvertDateTimeOffsetToTimestamp(v[i].Value,
                                    (TDenginePrecision)fields[colIndex].precision);
                            }
                        }

                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, vv,
                            TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                    }
                    else
                    {
                        throw new ArgumentException($"Unsupported timestamp type: {elementType}");
                    }

                    continue;
                }

                if ((TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                    (TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_JSONTAG)
                {
                    if (elementType == typeof(byte[]))
                    {
                        WriteUTF8(data, colInfoData, lengthData, rows, (byte[][])array,
                            (TDengineDataType)fields[colIndex].type);
                    }
                    else if (elementType == typeof(string))
                    {
                        WriteUTF8(data, colInfoData, lengthData, rows, (string[])array,
                            (TDengineDataType)fields[colIndex].type);
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"Unsupported binary type: {elementType},db type {(TDengineDataType)fields[colIndex].type}");
                    }

                    continue;
                }

                if ((TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_VARBINARY ||
                    (TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_GEOMETRY)
                {
                    if (elementType == typeof(byte[]))
                    {
                        WriteUTF8(data, colInfoData, lengthData, rows, (byte[][])array,
                            (TDengineDataType)fields[colIndex].type);
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"Unsupported varbinary/geometry type: {elementType},db type {(TDengineDataType)fields[colIndex].type}");
                    }

                    continue;
                }

                if ((TDengineDataType)fields[colIndex].type == TDengineDataType.TSDB_DATA_TYPE_NCHAR)
                {
                    if (elementType == typeof(byte[]))
                    {
                        WriteUTF32(data, colInfoData, lengthData, rows, (byte[][])array,
                            (TDengineDataType)fields[colIndex].type);
                    }
                    else if (elementType == typeof(string))
                    {
                        WriteUTF32(data, colInfoData, lengthData, rows, (string[])array,
                            (TDengineDataType)fields[colIndex].type);
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"Unsupported nchar type: {elementType}");
                    }

                    continue;
                }

                switch (elementType)
                {
                    case Type byteType when byteType == typeof(bool?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (bool?[])array);
                        break;
                    case Type byteType when byteType == typeof(bool):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (bool[])array);
                        break;
                    case Type byteType when byteType == typeof(sbyte?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (sbyte?[])array);
                        break;
                    case Type byteType when byteType == typeof(sbyte):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (sbyte[])array);
                        break;
                    case Type byteType when byteType == typeof(short):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (short[])array);
                        break;
                    case Type byteType when byteType == typeof(short?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (short?[])array);
                        break;
                    case Type byteType when byteType == typeof(int):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (int[])array);
                        break;
                    case Type byteType when byteType == typeof(int?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (int?[])array);
                        break;
                    case Type byteType when byteType == typeof(long):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long[])array);
                        break;
                    case Type byteType when byteType == typeof(long?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long?[])array);
                        break;
                    case Type byteType when byteType == typeof(byte):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (byte[])array);
                        break;
                    case Type byteType when byteType == typeof(byte?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (byte?[])array);
                        break;
                    case Type byteType when byteType == typeof(ushort):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ushort[])array);
                        break;
                    case Type byteType when byteType == typeof(ushort?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ushort?[])array);
                        break;
                    case Type byteType when byteType == typeof(uint):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (uint[])array);
                        break;
                    case Type byteType when byteType == typeof(uint?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (uint?[])array);
                        break;
                    case Type byteType when byteType == typeof(ulong):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ulong[])array);
                        break;
                    case Type byteType when byteType == typeof(ulong?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ulong?[])array);
                        break;
                    case Type byteType when byteType == typeof(float):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (float[])array);
                        break;
                    case Type byteType when byteType == typeof(float?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (float?[])array);
                        break;
                    case Type byteType when byteType == typeof(double):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (double[])array);
                        break;
                    case Type byteType when byteType == typeof(double?):
                        WriteData(data, colInfoData, lengthData, rows, bitMapLen, (double?[])array);
                        break;
                }
            }

            block.AddRange(colInfoData);
            block.AddRange(lengthData);
            block.AddRange(data);
            var blockLength = block.Count;
            for (int i = 0; i < TDengineConstant.Int32Size; i++)
            {
                block[4 + i] = (byte)(blockLength >> (8 * i));
            }

            return block.ToArray();
        }

        private static void AppendUint16(List<byte> bytes, ushort value)
        {
            bytes.Add((byte)value);
            bytes.Add((byte)(value >> 8));
        }

        private static void AppendUint32(List<byte> bytes, uint value)
        {
            bytes.Add((byte)value);
            bytes.Add((byte)(value >> 8));
            bytes.Add((byte)(value >> 16));
            bytes.Add((byte)(value >> 24));
        }

        private static void AppendUint64(List<byte> bytes, ulong value)
        {
            bytes.Add((byte)value);
            bytes.Add((byte)(value >> 8));
            bytes.Add((byte)(value >> 16));
            bytes.Add((byte)(value >> 24));
            bytes.Add((byte)(value >> 32));
            bytes.Add((byte)(value >> 40));
            bytes.Add((byte)(value >> 48));
            bytes.Add((byte)(value >> 56));
        }

        private static byte BMSetNull(byte c, int n)
        {
            return (byte)(c + (1 << (7 - TDengineConstant.BitPos(n))));
        }

        private static void WriteData<T>(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            int bitMapLen, T?[] value, TDengineDataType type)
            where T : struct
        {
            colInfoData.Add((byte)type);
            var length = TDengineConstant.TypeLengthMap[type];
            AppendUint32(colInfoData, (uint)length);
            AppendUint32(lengthData, (uint)(length * rows));
            var dataTmp = new byte[bitMapLen + rows * length];
            for (int rowIndex = 0; rowIndex < rows; rowIndex++)
            {
                if (value[rowIndex] == null)
                {
                    var charOffset = TDengineConstant.CharOffset(rowIndex);
                    dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex);
                }
                else
                {
                    var bytesToAdd = ConvertToBytes(value[rowIndex].Value, length);
                    Array.Copy(bytesToAdd, 0, dataTmp, rowIndex * length + bitMapLen, length);
                }
            }

            bytes.AddRange(dataTmp);
        }

        private static void WriteData<T>(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            int bitMapLen, T[] value, TDengineDataType type)
            where T : struct
        {
            colInfoData.Add((byte)type);
            var length = TDengineConstant.TypeLengthMap[type];
            AppendUint32(colInfoData, (uint)length);
            AppendUint32(lengthData, (uint)(length * rows));
            var dataTmp = new byte[bitMapLen + rows * length];
            for (int rowIndex = 0; rowIndex < rows; rowIndex++)
            {
                var bytesToAdd = ConvertToBytes(value[rowIndex], length);
                Array.Copy(bytesToAdd, 0, dataTmp, rowIndex * length + bitMapLen, length);
            }

            bytes.AddRange(dataTmp);
        }

        private static void WriteData<T>(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            int bitMapLen, T?[] value)
            where T : struct
        {
            var type = GetDataType<T>();
            WriteData(bytes, colInfoData, lengthData, rows, bitMapLen, value, type);
        }

        private static void WriteData<T>(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            int bitMapLen, T[] value)
            where T : struct
        {
            var type = GetDataType<T>();
            WriteData(bytes, colInfoData, lengthData, rows, bitMapLen, value, type);
        }

        private static void WriteUTF8(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            string[] value, TDengineDataType type)
        {
            WriteVarBinary(bytes, colInfoData, lengthData, rows, value, type, v => Encoding.UTF8.GetBytes(v));
        }

        private static void WriteUTF8(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            byte[][] value, TDengineDataType type)
        {
            WriteVarBinary(bytes, colInfoData, lengthData, rows, value, type, v => v);
        }

        private static void WriteUTF32(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            string[] value, TDengineDataType type)
        {
            WriteVarBinary(bytes, colInfoData, lengthData, rows, value, type, v => Encoding.UTF32.GetBytes(v));
        }

        private static void WriteUTF32(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            byte[][] value, TDengineDataType type)
        {
            WriteVarBinary(bytes, colInfoData, lengthData, rows, value, type,
                v => Encoding.Convert(Encoding.UTF32, Encoding.UTF8, v));
        }

        private static void WriteVarBinary<T>(List<byte> bytes, List<byte> colInfoData, List<byte> lengthData, int rows,
            T[] value, TDengineDataType type, Func<T, byte[]> stringToBytes)
        {
            colInfoData.Add((byte)type);
            AppendUint32(colInfoData, 0);
            var length = 0;
            var dataTmp = new List<byte>(TDengineConstant.Int32Size * rows);
            dataTmp.AddRange(new byte[TDengineConstant.Int32Size * rows]);
            for (int rowIndex = 0; rowIndex < rows; rowIndex++)
            {
                var offset = TDengineConstant.Int32Size * rowIndex;
                if (value[rowIndex] == null)
                {
                    for (int i = 0; i < TDengineConstant.Int32Size; i++)
                    {
                        dataTmp[offset + i] = 255;
                    }
                }
                else
                {
                    for (int i = 0; i < TDengineConstant.Int32Size; i++)
                    {
                        dataTmp[offset + i] = (byte)(length >> (8 * i));
                    }

                    var v = stringToBytes(value[rowIndex]);
                    AppendUint16(dataTmp, (ushort)v.Length);
                    dataTmp.AddRange(v);
                    length += v.Length + TDengineConstant.Int16Size;
                }
            }

            AppendUint32(lengthData, (uint)(length));
            bytes.AddRange(dataTmp);
        }

        private static TDengineDataType GetDataType<T>()
        {
            if (typeof(T) == typeof(bool)) return TDengineDataType.TSDB_DATA_TYPE_BOOL;
            if (typeof(T) == typeof(sbyte)) return TDengineDataType.TSDB_DATA_TYPE_TINYINT;
            if (typeof(T) == typeof(short)) return TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
            if (typeof(T) == typeof(int)) return TDengineDataType.TSDB_DATA_TYPE_INT;
            if (typeof(T) == typeof(long)) return TDengineDataType.TSDB_DATA_TYPE_BIGINT;
            if (typeof(T) == typeof(byte)) return TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
            if (typeof(T) == typeof(ushort)) return TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
            if (typeof(T) == typeof(uint)) return TDengineDataType.TSDB_DATA_TYPE_UINT;
            if (typeof(T) == typeof(ulong)) return TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
            if (typeof(T) == typeof(float)) return TDengineDataType.TSDB_DATA_TYPE_FLOAT;
            if (typeof(T) == typeof(double)) return TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
            throw new ArgumentException($"Unsupported data type: {typeof(T)}");
        }

        private static byte[] ConvertToBytes<T>(T value, int size)
        {
            byte[] byteArray = new byte[size];
            IntPtr ptr = Marshal.AllocHGlobal(size);
            Marshal.StructureToPtr(value, ptr, true);
            Marshal.Copy(ptr, byteArray, 0, size);
            Marshal.FreeHGlobal(ptr);
            return byteArray;
        }
    }
}
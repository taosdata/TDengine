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
                switch ((TDengineDataType)fields[colIndex].type)
                {
                    // timestamp
                    case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    {
                        if (elementType == typeof(long))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long[])array,
                                TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                        }
                        else if (elementType == typeof(long?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long?[])array,
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
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, TIMESTAMP database type requires one of the following types: DateTime, DateTime?, long, long?, DateTimeOffset, DateTimeOffset?, but got {elementType.Name}");
                        }

                        break;
                    }
                    // binary, json, varbinary
                    case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    case TDengineDataType.TSDB_DATA_TYPE_VARBINARY:
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
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, {(TDengineDataType)fields[colIndex].type} database type requires byte[] or string, but got {elementType.Name}");
                        }

                        break;
                    }
                    // geometry
                    case TDengineDataType.TSDB_DATA_TYPE_GEOMETRY:
                    {
                        if (elementType == typeof(byte[]))
                        {
                            WriteUTF8(data, colInfoData, lengthData, rows, (byte[][])array,
                                (TDengineDataType)fields[colIndex].type);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, GEOMETRY database type requires byte[], but got {elementType.Name}");
                        }

                        break;
                    }
                    // nchar
                    case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    {
                        if (elementType == typeof(string))
                        {
                            WriteUTF32(data, colInfoData, lengthData, rows, (string[])array,
                                (TDengineDataType)fields[colIndex].type);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, NCHAR database type requires string, but got {elementType.Name}");
                        }

                        break;
                    }
                    // bool
                    case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    {
                        if (elementType == typeof(bool?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (bool?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_BOOL);
                        }
                        else if (elementType == typeof(bool))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (bool[])array,
                                TDengineDataType.TSDB_DATA_TYPE_BOOL);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, BOOL database type requires bool or bool?, but got {elementType.Name}");
                        }

                        break;
                    }
                    // tinyint
                    case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    {
                        if (elementType == typeof(sbyte?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (sbyte?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_TINYINT);
                        }
                        else if (elementType == typeof(sbyte))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (sbyte[])array,
                                TDengineDataType.TSDB_DATA_TYPE_TINYINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, TINYINT database type requires sbyte or sbyte?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // smallint
                    case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    {
                        if (elementType == typeof(short?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (short?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
                        }
                        else if (elementType == typeof(short))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (short[])array,
                                TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, SMALLINT database type requires short or short?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // int
                    case TDengineDataType.TSDB_DATA_TYPE_INT:
                    {
                        if (elementType == typeof(int?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (int?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_INT);
                        }
                        else if (elementType == typeof(int))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (int[])array,
                                TDengineDataType.TSDB_DATA_TYPE_INT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, INT database type requires int or int?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // bigint
                    case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    {
                        if (elementType == typeof(long?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_BIGINT);
                        }
                        else if (elementType == typeof(long))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (long[])array,
                                TDengineDataType.TSDB_DATA_TYPE_BIGINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, BIGINT database type requires long or long?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // utinyint
                    case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    {
                        if (elementType == typeof(byte?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (byte?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
                        }
                        else if (elementType == typeof(byte))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (byte[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                            $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, TINYINT UNSIGNED database type requires byte or byte?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // usmallint
                    case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    {
                        if (elementType == typeof(ushort?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ushort?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
                        }
                        else if (elementType == typeof(ushort))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ushort[])array,
                                TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, SMALLINT UNSIGNED database type requires ushort or ushort?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // uint
                    case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    {
                        if (elementType == typeof(uint?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (uint?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UINT);
                        }
                        else if (elementType == typeof(uint))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (uint[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, INT UNSIGNED database type requires uint or uint?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // ubigint
                    case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    {
                        if (elementType == typeof(ulong?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ulong?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
                        }
                        else if (elementType == typeof(ulong))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (ulong[])array,
                                TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, BIGINT UNSIGNED database type requires uint or uint?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // float
                    case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    {
                        if (elementType == typeof(float?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (float?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_FLOAT);
                        }
                        else if (elementType == typeof(float))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (float[])array,
                                TDengineDataType.TSDB_DATA_TYPE_FLOAT);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, FLOAT database type requires float or float?, but got {elementType.Name}");
                        }
                        break;
                    }
                    // double
                    case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    {
                        if (elementType == typeof(double?))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (double?[])array,
                                TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
                        }
                        else if (elementType == typeof(double))
                        {
                            WriteData(data, colInfoData, lengthData, rows, bitMapLen, (double[])array,
                                TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {colIndex}, field name: {fields[colIndex].name}, DOUBLE database type requires double or double?, but got {elementType.Name}");
                        }
                        break;
                    }
                    default:
                        throw new ArgumentException($"Can not bind data for database type: {TDengineConstant.GetFieldTypeName(fields[colIndex].type)}");
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
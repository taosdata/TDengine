using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        private void CheckColumns(Array array, TaosFieldE field, int bindIndex)
        {
            var elementType = array.GetType().GetElementType();
            if (elementType == null)
            {
                throw new ArgumentException(
                    $"BindIndex: {bindIndex}, field name: {field.name}, Expected an array type, but received {array.GetType().Name}");
            }

            switch ((TDengineDataType)field.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    if (elementType != typeof(bool?) && elementType != typeof(bool))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, BOOL database type requires bool[] or bool?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    if (elementType != typeof(sbyte?) && elementType != typeof(sbyte))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT database type requires sbyte[] or sbyte?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    if (elementType != typeof(short?) && elementType != typeof(short))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT database type requires short[] or short?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    if (elementType != typeof(int?) && elementType != typeof(int))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, INT database type requires int[] or int?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    if (elementType != typeof(long?) && elementType != typeof(long))

                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT database type requires long[] or long?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    if (elementType != typeof(float?) && elementType != typeof(float))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, FLOAT database type requires float[] or float?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    if (elementType != typeof(double?) && elementType != typeof(double))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, DOUBLE database type requires double[] or double?[], but got an array of {elementType.Name}");
                    }

                    break;

                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                case TDengineDataType.TSDB_DATA_TYPE_VARBINARY:
                    if (elementType != typeof(byte[]) && elementType != typeof(string))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type requires byte[][] or string[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    if (elementType != typeof(DateTime?) && elementType != typeof(DateTime) &&
                        elementType != typeof(long?) &&
                        elementType != typeof(long) && elementType != typeof(DateTimeOffset?) &&
                        elementType != typeof(DateTimeOffset))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, TIMESTAMP database type requires one of the following array types: DateTime[], DateTime?[], long[], long?[], DateTimeOffset[], DateTimeOffset?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    if (elementType != typeof(string))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, NCHAR database type requires string[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    if (elementType != typeof(byte?) && elementType != typeof(byte))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, UTINYINT database type requires byte[] or byte?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    if (elementType != typeof(ushort?) && elementType != typeof(ushort))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, USMALLINT database type requires ushort[] or ushort?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    if (elementType != typeof(uint?) && elementType != typeof(uint))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, UINT database type requires uint[] or uint?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    if (elementType != typeof(ulong?) && elementType != typeof(ulong))

                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, UBIGINT database type requires ulong[] or ulong?[], but got an array of {elementType.Name}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_GEOMETRY:
                    if (elementType != typeof(byte[]))
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, GEOMETRY database type requires byte[][], but got an array of {elementType.Name}");
                    }

                    break;
                default:
                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type not supported");
            }
        }

        public void BindColumn(TaosFieldE[] _, params Array[] arrays)
        {
            CheckPrepared();
            CheckTableNameSet();
            if (_isInsert)
            {
                if (arrays == null || arrays.Length == 0)
                {
                    throw new ArgumentException("Bind columns cannot be null or empty");
                }
                if (_colFields.Length != arrays.Length)
                {
                    throw new ArgumentException(
                        $"Expected {_colFields.Length} columns, but got {arrays.Length}");
                }

                var rowCount = arrays[0].Length;
                if (rowCount == 0)
                {
                    throw new ArgumentException($"Expected non-empty arrays, but got empty array");
                }

                for (var i = 0; i < arrays.Length; i++)
                {
                    if (arrays[i].Length != rowCount)
                    {
                        throw new ArgumentException(
                            $"All arrays must have the same length. Expected length {rowCount}, but got array at index {i} with length {arrays[i].Length}");
                    }
                    CheckColumns(arrays[i], _colFields[i],i);
                }

                for (int i = 0; i < arrays.Length; i++)
                {
                    for (var j = 0; j < arrays[i].Length; j++)
                    {
                        _currentTableInfo.Cols[i].Add(arrays[i].GetValue(j));
                    }
                }

                IsColSet = true;
            }
            else
            {
                throw new InvalidOperationException(
                    "Does not support binding columns for non-insert statements.");
            }
        }
    }
}
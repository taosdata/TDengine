using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        private static void CheckRowValue(object[] obj, TaosFieldE[] fields)
        {
            for (var i = 0; i < fields.Length; i++)
            {
                if (obj[i] != null && !Convert.IsDBNull(obj[i]))
                {
                    switch (obj[i])
                    {
                        case bool _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BOOL)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type bool to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case sbyte _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type sbyte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case short _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case int _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_INT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case long _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type long to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case byte _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case ushort _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type ushort to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case uint _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type uint to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case ulong _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type ulong to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case float _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type float to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case double _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type double to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case DateTime _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type DateTime to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case DateTimeOffset _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                            {
                                throw new ArgumentException(
                                    $"Bind type DateTimeOffset not supported for field {fields[i].name}");
                            }

                            break;
                        case byte[] _:
                            if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BINARY &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_GEOMETRY
                               )
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte[] to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        case string _:
                            if (
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BINARY &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY &&
                                fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR
                            )
                            {
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, bind param type string to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                            }

                            break;
                        default:
                            throw new ArgumentException(
                                $"BindIndex: {i}, field name: {fields[i].name}, stmt bind param type not supported: {obj[i].GetType()}");
                    }
                }
            }
        }

        public void BindRow(object[] row)
        {
            CheckPrepared();
            CheckTableNameSet();
            if (row == null || row.Length == 0)
            {
                throw new ArgumentException("Row cannot be null or empty");
            }

            if (_isInsert)
            {
                if (row.Length != _colFields.Length)
                {
                    throw new ArgumentException(
                        $"Expected {_colFields.Length} columns, but got {row.Length}");
                }

                CheckRowValue(row, _colFields);
                for (var i = 0; i < row.Length; i++)
                {
                    _currentTableInfo.Cols[i].Add(row[i]);
                }
            }
            else
            {
                if (row.Length != _fieldsCount)
                {
                    throw new ArgumentException(
                        $"Expected {_fieldsCount} fields, but got {row.Length}");
                }

                if (_currentTableInfo.IsColSet)
                {
                    throw new InvalidOperationException("Query parameters have already been set.");
                }

                var fields = new TaosFieldE[row.Length];
                for (var i = 0; i < row.Length; i++)
                {
                    try
                    {
                        if (row[i] == null || Convert.IsDBNull(row[i]))
                        {
                            throw new ArgumentException("query parameter cannot be null or DBNull");
                        }

                        switch (row[i])
                        {
                            case DateTime dt:
                                _currentTableInfo.Cols[i].Add(dt.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                                };
                                break;
                            case DateTimeOffset dto:
                                _currentTableInfo.Cols[i].Add(dto.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                                };
                                break;
                            case sbyte _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                                    bytes = 1
                                };
                                break;
                            case short _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_SMALLINT,
                                    bytes = 2
                                };
                                break;
                            case int _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_INT,
                                    bytes = 4
                                };
                                break;
                            case long _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                                };
                                break;
                            case byte _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                                };
                                break;
                            case ushort _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                                };
                                break;
                            case uint _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UINT,
                                };
                                break;
                            case ulong _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                                };
                                break;
                            case float _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                                };
                                break;
                            case double _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                                };
                                break;
                            case string _:
                            case byte[] _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                                };
                                break;
                            case bool _:
                                _currentTableInfo.Cols[i].Add(row[i]);
                                fields[i] = new TaosFieldE
                                {
                                    type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BOOL,
                                };
                                break;
                            default:
                                throw new ArgumentException(
                                    $"BindIndex: {i}, stmt bind query param type not supported: {row[i].GetType()}");
                        }
                    }
                    catch
                    {
                        for (var j = 0; j < i; j++)
                        {
                            _currentTableInfo.Cols[j].RemoveAt(_currentTableInfo.Cols[j].Count - 1);
                        }
                        throw;
                    }
                }

                _queryFields = fields;
            }


            _isColSet = true;
        }
    }
}
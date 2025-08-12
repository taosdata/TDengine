using System;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSStmt : IStmt
    {
        private readonly ulong _stmt;
        private readonly TimeZoneInfo _tz;
        private readonly Connection _connection;
        private bool _closed;
        private long _lastAffected;
        private bool _isInsert;

        public WSStmt(ulong stmt, TimeZoneInfo tz, Connection connection)
        {
            _stmt = stmt;
            _tz = tz;
            _connection = connection;
        }


        public void Dispose()
        {
            if (_closed) return;

            _closed = true;
            if (_connection == null || !_connection.IsAvailable()) return;
            try
            {
                _connection.StmtClose(_stmt);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public void Prepare(string query)
        {
            var resp = _connection.StmtPrepare(_stmt, query);
            _isInsert = resp.IsInsert;
        }

        public bool IsInsert()
        {
            return _isInsert;
        }

        public void SetTableName(string tableName)
        {
            _connection.StmtSetTableName(_stmt, tableName);
        }

        public void SetTags(object[] tags)
        {
            var fields = GetTagFields();
            _connection.StmtSetTags(_stmt, fields, tags);
        }

        public TaosFieldE[] GetTagFields()
        {
            var resp = _connection.StmtGetTagFields(_stmt);
            TaosFieldE[] fields = new TaosFieldE[resp.Fields.Count];
            for (int i = 0; i < resp.Fields.Count; i++)
            {
                fields[i] = new TaosFieldE
                {
                    name = resp.Fields[i].Name,
                    type = resp.Fields[i].FieldType,
                    precision = resp.Fields[i].Precision,
                    scale = resp.Fields[i].Scale,
                    bytes = resp.Fields[i].Bytes
                };
            }

            return fields;
        }

        public TaosFieldE[] GetColFields()
        {
            var resp = _connection.StmtGetColFields(_stmt);
            TaosFieldE[] fields = new TaosFieldE[resp.Fields.Count];
            for (int i = 0; i < resp.Fields.Count; i++)
            {
                fields[i] = new TaosFieldE
                {
                    name = resp.Fields[i].Name,
                    type = resp.Fields[i].FieldType,
                    precision = resp.Fields[i].Precision,
                    scale = resp.Fields[i].Scale,
                    bytes = resp.Fields[i].Bytes
                };
            }

            return fields;
        }

        public void BindRow(object[] row)
        {
            if (IsInsert())
            {
                _connection.StmtBind(_stmt, GetColFields(), row);
            }
            else
            {
                var tmpRow = new object[row.Length];
                Array.Copy(row, tmpRow, row.Length);
                _connection.StmtBind(_stmt, GenerateStmtQueryColFields(tmpRow), tmpRow);
            }
        }

        private TaosFieldE[] GenerateStmtQueryColFields(object[] row)
        {
            var result = new TaosFieldE[row.Length];
            for (int i = 0; i < row.Length; i++)
            {
                switch (row[i])
                {
                    case bool _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BOOL
                        };
                        break;
                    case sbyte _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TINYINT
                        };
                        break;
                    case short _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_SMALLINT
                        };
                        break;
                    case int _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_INT
                        };
                        break;
                    case long _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BIGINT
                        };
                        break;
                    case byte _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UTINYINT
                        };
                        break;
                    case ushort _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_USMALLINT
                        };
                        break;
                    case uint _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UINT
                        };
                        break;
                    case ulong _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UBIGINT
                        };
                        break;
                    case float _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_FLOAT
                        };
                        break;
                    case double _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_DOUBLE
                        };
                        break;
                    case byte[] _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY
                        };
                        break;
                    case DateTime val:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY
                        };
                        var time = val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK");
                        row[i] = time;
                        break;
                    case DateTimeOffset val:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY
                        };
                        var timeOffset = val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK");
                        row[i] = timeOffset;
                        break;
                    case string _:
                        result[i] = new TaosFieldE
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY
                        };
                        break;
                    default:
                        throw new ArgumentException("Unsupported type, only support basic types and DateTime");
                }
            }

            return result;
        }

        public void BindColumn(TaosFieldE[] field, params Array[] arrays)
        {
            _connection.StmtBind(_stmt, field, arrays);
        }

        public void AddBatch()
        {
            _connection.StmtAddBatch(_stmt);
        }

        public void Exec()
        {
            var resp = _connection.StmtExec(_stmt);
            _lastAffected = resp.Affected;
        }

        public long Affected()
        {
            return _lastAffected;
        }

        public IRows Result()
        {
            if (IsInsert())
            {
                return new WSRows((int)Affected());
            }

            var resp = _connection.StmtUseResult(_stmt);
            return new WSRows(resp.ResultId, resp, _connection, _tz);
        }
    }
}
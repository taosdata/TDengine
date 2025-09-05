using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using TDengine.Driver;

namespace TDengine.Data.Client
{
    public class TDengineCommand : DbCommand
    {
        private readonly Lazy<TDengineParameterCollection> _parameters =
            new Lazy<TDengineParameterCollection>(() => new TDengineParameterCollection());

        private TDengineConnection _connection;
        private string _commandText;
        private bool _isPrepared;
        private IStmt _stmt;

        public TDengineCommand()
        {
        }

        public TDengineCommand(TDengineConnection connection)
        {
            _connection = connection;
        }

        public override void Cancel()
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (_stmt != null)
            {
                _stmt.Dispose();
                _stmt = null;
            }

            base.Dispose(disposing);
        }

        public override int ExecuteNonQuery()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteNonQuery)}");
            }

            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteNonQuery)}");
            }

            int result;
            using (var rows = Statement())
            {
                result = rows.AffectRows;
            }

            return result;
        }

        public override object ExecuteScalar()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteScalar)}");
            }

            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteScalar)}");
            }

            object result;
            using (var rows = Statement())
            {
                result = rows.Read() ? rows.GetValue(0) : null;
            }

            return result;
        }

        public override void Prepare()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(Prepare)}");
            }

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(Prepare)}");
            }
        }

        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (_stmt != null)
                {
                    // if the statement is not null and need to be re-prepared,
                    // we need to dispose the old statement and create a new one.
                    // this is a bug workaround for TDengine 3.3.6.0
                    _stmt.Dispose();
                    _stmt = null;
                }
                _isPrepared = false;
                _commandText = value;
            }
        }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentException($"Invalid CommandType{value}");
                }
            }
        }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => _connection;
            set => _connection = value as TDengineConnection ??
                                 throw new ArgumentException(
                                     $"The specified connection is not of type {nameof(TDengineConnection)}.");
        }

        protected override DbParameterCollection DbParameterCollection => _parameters.Value;
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }

        protected override DbParameter CreateDbParameter() => new TDengineParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var rows = Statement();
            return new TDengineDataReader(rows);
        }

        public new virtual TDengineParameterCollection Parameters => _parameters.Value;

        private IRows Query()
        {
            return _connection.Client.Query(_commandText);
        }

        private IRows Statement()
        {
            if (!_parameters.IsValueCreated || _parameters.Value.Count == 0)
            {
                return Query();
            }

            if (_connection == null) throw new InvalidOperationException("Connection is null");

            if (_stmt == null)
            {
                _stmt = _connection.Client.StmtInit();
            }
            
            else if (!_isPrepared)
            {
                
                _stmt.Dispose();
                _stmt = _connection.Client.StmtInit();
            }

            if (_stmt == null) throw new InvalidOperationException("Statement is null");

            if (!_isPrepared)
            {
                _stmt.Prepare(_commandText);
                _isPrepared = true;
            }

            var isInsert = _stmt.IsInsert();

            var pms = _parameters.Value;
            var tableName = string.Empty;
            List<object> tags = new List<object>();
            List<object> data = new List<object>();
            for (int i = 0; i < pms.Count; i++)
            {
                var parameter = pms[i];
                if (parameter.ParameterName.StartsWith("$"))
                {
                    tags.Add(parameter.Value);
                }
                else if (parameter.ParameterName.StartsWith("@"))
                {
                    data.Add(parameter.Value);
                }
                else if (parameter.ParameterName.StartsWith("#"))
                {
                    if (string.IsNullOrEmpty(tableName))
                    {
                        tableName = parameter.Value as string;
                    }
                    else
                    {
                        throw new ArgumentException("table name already set");
                    }
                }
                else
                {
                    throw new ArgumentException($"Invalid parameter name: {parameter.ParameterName}, " +
                                                $"parameter name should start with $, @ or #");
                }
            }

            if (isInsert)
            {
                if (!string.IsNullOrEmpty(tableName))
                {
                    _stmt.SetTableName(tableName);
                }

                if (tags.Count > 0)
                {
                    _stmt.SetTags(tags.ToArray());
                }

                if (data.Count > 0)
                {
                    _stmt.BindRow(data.ToArray());
                }
            }
            else
            {
                if (data.Count > 0)
                {
                    _stmt.BindRow(data.ToArray());
                }
            }

            _stmt.AddBatch();
            _stmt.Exec();
            return _stmt.Result();
        }
    }
}
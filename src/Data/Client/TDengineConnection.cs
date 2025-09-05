using System;
using System.Data;
using System.Data.Common;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;


namespace TDengine.Data.Client
{
    public class TDengineConnection : DbConnection
    {
        private string _connectionString;
        private ConnectionState _state;

        internal ITDengineClient Client;

        // internal object connection;
        public TDengineConnectionStringBuilder ConnectionStringBuilder { get; set; }

        public TDengineConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            Client.Exec("use `" + databaseName + "`;");
        }

        public override void Close()
        {
            if (State == ConnectionState.Closed) return;
            if (Client != null)
            {
                Client.Dispose();
                Client = null;
            }

            SetState(ConnectionState.Closed);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new TDengineCommand(this);
        }

        private void SetState(ConnectionState value)
        {
            var originalState = _state;
            if (originalState == value) return;
            _state = value;
            OnStateChange(new StateChangeEventArgs(originalState, value));
        }

        public override void Open()
        {
            if (State == ConnectionState.Open) return;
            Client = DbDriver.Open(ConnectionStringBuilder);
            SetState(ConnectionState.Open);
        }

        public sealed override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (State == ConnectionState.Open)
                {
                    throw new InvalidOperationException("Cannot change connection string on an open connection");
                }

                _connectionString = value;
                ConnectionStringBuilder = new TDengineConnectionStringBuilder(value);
            }
        }

        public override string Database
        {
            get
            {
                if (State != ConnectionState.Open) return ConnectionStringBuilder.Database;
                try
                {
                    using (var rows = Client.Query("select database();"))
                    {
                        return rows.Read() ? rows.GetString(0) : string.Empty;
                    }
                }
                catch
                {
                    return string.Empty;
                }
            }
        }

        public override ConnectionState State
        {
            get
            {
                if (_state != ConnectionState.Open) return _state;
                var client = Client;
                if (client != null && !client.ConnectionAvailable())
                {
                    SetState(ConnectionState.Broken);
                }

                return _state;
            }
        }

        public override string DataSource => ConnectionStringBuilder.Host;

        public override string ServerVersion
        {
            get
            {
                try
                {
                    using (var rows = Client.Query("select server_version();"))
                    {
                        return rows.Read() ? rows.GetString(0) : string.Empty;
                    }
                }
                catch
                {
                    return string.Empty;
                }
            }
        }
    }
}
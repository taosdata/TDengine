using System;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSStmt : AbstractStmt
    {
        private readonly WSClient _client;
        private readonly TimeZoneInfo _tz;
        private Connection _connection;
        private ulong _stmt;
        private bool _closed;

        public WSStmt(WSClient client, ulong stmt, TimeZoneInfo tz, Connection connection) : base(30)
        {
            _client = client;
            _stmt = stmt;
            _tz = tz;
            _connection = connection;
        }

        public override void Dispose()
        {
            if (_closed) return;

            _closed = true;
            if (_connection == null || !_connection.IsAvailable()) return;
            try
            {
                _connection.Stmt2Close(_stmt);
            }
            catch (Exception)
            {
                // ignored
            }
            finally
            {
                _connection = null;
            }
        }

        protected override void PrepareInternal(string query, out bool isInsert, out int count,
            out TaosFieldAll[] fields)
        {
            var resp = _connection.Stmt2Prepare(_stmt, query);
            isInsert = resp.IsInsert;
            count = resp.FieldsCount;
            if (!isInsert)
            {
                fields = null;
                return;
            }

            fields = new TaosFieldAll[resp.Fields.Count];
            for (int i = 0; i < resp.Fields.Count; i++)
            {
                fields[i] = new TaosFieldAll
                {
                    name = resp.Fields[i].Name,
                    type = resp.Fields[i].FieldType,
                    precision = resp.Fields[i].Precision,
                    scale = resp.Fields[i].Scale,
                    bytes = resp.Fields[i].Bytes,
                    field_type = resp.Fields[i].BindType
                };
            }
        }

        protected override void BindBinaryInternal(byte[] data, out int affectedRows)
        {
            _connection.Stmt2Bind(_stmt, data);
            var resp = _connection.Stmt2Exec(_stmt);
            affectedRows = resp.Affected;
        }

        protected override bool IsConnectionAvailable(Exception exception)
        {
            return _connection != null && _connection.IsAvailable(exception);
        }

        protected override  void ReconnectInternal()
        {
            _stmt = 0;
            var newConnection = _client.TryReconnectOrGetConnection(_connection);
            _connection = newConnection;
            // init again
            var resp = _connection.Stmt2Init((ulong)ReqId.GetReqId());
            _stmt = resp.StmtId;
        }
        
        protected override bool AutoReconnectInternal()
        {
            return _client.AutoReconnect;
        }

        protected override IRows QueryResultInternal()
        {
            var resp = _connection.Stmt2UseResult(_stmt);
            return new WSRows(resp.ResultId, resp, _connection, _tz);
        }

        protected override IRows InsertResultInternal(int affectedRows)
        {
            return new WSRows(affectedRows);
        }
    }
}
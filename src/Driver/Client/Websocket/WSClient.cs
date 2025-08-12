using System;
using System.Diagnostics;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSClient : ITDengineClient
    {
        private Connection _connection;
        private readonly TimeZoneInfo _tz;
        private readonly ConnectionStringBuilder _builder;
        private readonly object _reconnectLock = new object();


        public WSClient(ConnectionStringBuilder builder)
        {
            Debug.Assert(builder.Protocol == TDengineConstant.ProtocolWebSocket);
            _tz = builder.GetTimeZone();
            _connection = new Connection(GetUrl(builder), builder.Username, builder.Password,
                builder.Database, builder.ConnTimeout, builder.ReadTimeout, builder.WriteTimeout,
                builder.EnableCompression,builder.ConnectionTimezone);

            _connection.Connect();
            _builder = builder;
        }

        public static string GetUrl(ConnectionStringBuilder builder)
        {
            var schema = "ws";
            var port = builder.Port;
            if (builder.UseSSL)
            {
                schema = "wss";
                if (builder.Port == 0)
                {
                    port = 443;
                }
            }
            else
            {
                if (builder.Port == 0)
                {
                    port = 6041;
                }
            }

            var token = builder.Token;
            var uriBuilder = new UriBuilder
            {
                Scheme = schema,
                Host = builder.Host,
                Port = port,
                Path = "/ws"
            };

            if (!string.IsNullOrEmpty(token))
            {
                uriBuilder.Query = $"token={token}";
            }

            return uriBuilder.ToString();
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Close();
                _connection = null;
            }
        }

        private void Reconnect()
        {
            if (!_builder.AutoReconnect)
                return;
            lock (_reconnectLock)
            {
                if (_connection != null && _connection.IsAvailable()) // connection is available, no need to reconnect
                    return;

                Connection connection = null;
                for (int i = 0; i < _builder.ReconnectRetryCount; i++)
                {
                    try
                    {
                        // sleep
                        System.Threading.Thread.Sleep(_builder.ReconnectIntervalMs);
                        connection = new Connection(GetUrl(_builder), _builder.Username, _builder.Password,
                            _builder.Database, _builder.ConnTimeout, _builder.ReadTimeout, _builder.WriteTimeout,
                            _builder.EnableCompression,_builder.ConnectionTimezone);
                        connection.Connect();
                        break;
                    }
                    catch (Exception)
                    {
                        if (connection != null)
                        {
                            connection.Close();
                            connection = null;
                        }
                    }
                }

                if (connection == null)
                {
                    throw new TDengineError((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED,
                        "websocket connection reconnect failed");
                }

                if (_connection != null)
                {
                    _connection.Close();
                }

                _connection = connection;
            }
        }

        public IStmt StmtInit()
        {
            return StmtInit(ReqId.GetReqId());
        }

        public IStmt StmtInit(long reqId)
        {
            try
            {
                return DoStmtInit(reqId);
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                return DoStmtInit(reqId);
            }
        }

        private IStmt DoStmtInit(long reqId)
        {
            var resp = _connection.StmtInit((ulong)reqId);
            return new WSStmt(resp.StmtId, _tz, _connection);
        }

        public IRows Query(string query)
        {
            return Query(query, ReqId.GetReqId());
        }

        public IRows Query(string query, long reqId)
        {
            try
            {
                return DoQuery(query, reqId);
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                return DoQuery(query, reqId);
            }
        }

        private IRows DoQuery(string query, long reqId)
        {
            var resp = _connection.BinaryQuery(query, (ulong)reqId);
            if (resp.IsUpdate)
            {
                return new WSRows(resp.AffectedRows);
            }

            return new WSRows(resp.ResultId, resp, _connection, _tz);
        }

        public long Exec(string query)
        {
            return Exec(query, ReqId.GetReqId());
        }

        public long Exec(string query, long reqId)
        {
            try
            {
                return DoExec(query, reqId);
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                return DoExec(query, reqId);
            }
        }

        private long DoExec(string query, long reqId)
        {
            var resp = _connection.BinaryQuery(query, (ulong)reqId);
            if (!resp.IsUpdate)
            {
                _connection.FreeResult(resp.ResultId);
            }

            return resp.AffectedRows;
        }

        public void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            try
            {
                DoSchemalessInsert(lines, protocol, precision, ttl, reqId);
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                DoSchemalessInsert(lines, protocol, precision, ttl, reqId);
            }
        }

        private void DoSchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            var line = string.Join("\n", lines);
            _connection.SchemalessInsert(line, protocol, precision, ttl, reqId);
        }
    }
}
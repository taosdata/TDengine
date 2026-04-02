using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSClient : ITDengineClient
    {
        private volatile Connection _connection;
        private volatile FailoverAddressLease _addressLease;
        private int _disposed;
        private readonly TimeZoneInfo _tz;
        private readonly ConnectionStringBuilder _builder;
        private readonly IReadOnlyList<FailoverAddress> _failoverAddresses;
        private readonly object _reconnectLock = new object();

        internal bool AutoReconnect => _builder.AutoReconnect;

        public WSClient(ConnectionStringBuilder builder)
        {
            Debug.Assert(builder.Protocol == TDengineConstant.ProtocolWebSocket);
            _builder = builder;
            _tz = builder.GetTimeZone();
            _failoverAddresses = builder.GetFailoverAddresses();

            if (!FailoverConnector.TryOpen(_failoverAddresses, 1, 0, false, null, OpenWsConnection,
                    out var connection, out var lease, out var lastException))
            {
                if (lastException != null)
                {
                    throw lastException;
                }

                throw new TDengineError((int)TDengineError.InternalErrorCode.WS_CONNECT_FAILED,
                    "websocket connection failed");
            }

            _connection = connection;
            _addressLease = lease;
        }

        public static string GetUrl(ConnectionStringBuilder builder)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            var addresses = builder.GetFailoverAddresses();
            if (addresses.Count == 0)
            {
                throw new ArgumentException("failover addresses is empty", nameof(builder));
            }

            var address = addresses[0];
            return GetUrl(builder, address.Host, address.Port);
        }

        internal static string GetUrl(ConnectionStringBuilder builder, string host, int port)
        {
            var schema = "ws";
            if (builder.UseSSL)
            {
                schema = "wss";
                if (port == 0)
                {
                    port = 443;
                }
            }
            else if (port == 0)
            {
                port = 6041;
            }

            var token = builder.Token;
            var uriBuilder = new UriBuilder
            {
                Scheme = schema,
                Host = host,
                Port = port,
                Path = "/ws"
            };

            if (!string.IsNullOrEmpty(token))
            {
                uriBuilder.Query = $"token={token}";
            }

            return uriBuilder.ToString();
        }

        private Connection CreateConnection(FailoverAddress address)
        {
            return new Connection(GetUrl(_builder, address.Host, address.Port), _builder.Username, _builder.Password,
                _builder.Database, _builder.BearerToken, _builder.ConnTimeout, _builder.ReadTimeout,
                _builder.WriteTimeout, _builder.EnableCompression, _builder.ConnectionTimezone);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            Connection oldConnection;
            FailoverAddressLease oldLease;

            lock (_reconnectLock)
            {
                oldConnection = _connection;
                oldLease = _addressLease;
                _connection = null;
                _addressLease = null;
            }

            oldConnection?.Close();

            oldLease?.Dispose();
        }

        private bool IsDisposed()
        {
            return Volatile.Read(ref _disposed) == 1;
        }

        private void ThrowIfDisposed()
        {
            if (IsDisposed())
            {
                throw new ObjectDisposedException(nameof(WSClient));
            }
        }

        private Connection OpenWsConnection(FailoverAddress address)
        {
            Connection currentConnection = null;
            try
            {
                currentConnection = CreateConnection(address);
                currentConnection.Connect();
                return currentConnection;
            }
            catch
            {
                if (currentConnection != null)
                {
                    currentConnection.Close();
                }

                throw;
            }
        }

        private void Reconnect(bool force = false, Connection old = null)
        {
            if (!AutoReconnect)
            {
                return;
            }

            ThrowIfDisposed();

            while (true)
            {
                FailoverAddress preferredAddress;
                lock (_reconnectLock)
                {
                    if (_connection != null && _connection.IsAvailable()) // connection is available, no need to reconnect
                    {
                        if (!force)
                        {
                            return;
                        }

                        if (old != null && _connection != old)
                        {
                            return; // another thread has reconnected
                        }
                    }

                    if (IsDisposed())
                    {
                        throw new ObjectDisposedException(nameof(WSClient));
                    }

                    preferredAddress = _addressLease == null ? null : _addressLease.Address;
                }

                if (!FailoverConnector.TryOpen(_failoverAddresses, _builder.ReconnectRetryCount,
                        _builder.ReconnectIntervalMs, true, preferredAddress, OpenWsConnection,
                        out var connection, out var lease, out var lastException))
                {
                    lock (_reconnectLock)
                    {
                        if (_connection != null && _connection.IsAvailable())
                        {
                            if (!force || (old != null && _connection != old))
                            {
                                return;
                            }
                        }

                        if (IsDisposed())
                        {
                            throw new ObjectDisposedException(nameof(WSClient));
                        }
                    }

                    var reason = lastException == null
                        ? "websocket connection reconnect failed"
                        : $"websocket connection reconnect failed: {lastException.Message}";
                    throw new TDengineError((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED, reason);
                }

                Connection oldConnection = null;
                FailoverAddressLease oldLease = null;
                var needDiscard = false;
                lock (_reconnectLock)
                {
                    if (IsDisposed())
                    {
                        needDiscard = true;
                    }
                    else if (_connection != null && _connection.IsAvailable())
                    {
                        if (!force || (old != null && _connection != old))
                        {
                            needDiscard = true;
                        }
                    }

                    if (!needDiscard)
                    {
                        oldConnection = _connection;
                        oldLease = _addressLease;
                        _connection = connection;
                        _addressLease = lease;
                    }
                }

                if (needDiscard)
                {
                    connection.Close();
                    lease.Dispose();
                    return;
                }

                if (oldConnection != null)
                {
                    oldConnection.Close();
                }

                if (oldLease != null)
                {
                    oldLease.Dispose();
                }

                return;
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
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfDisposed();
                Reconnect();
                return DoStmtInit(reqId);
            }
        }

        private IStmt DoStmtInit(long reqId)
        {
            var connection = _connection;
            if (connection == null)
            {
                throw new ObjectDisposedException(nameof(WSClient));
            }

            var resp = connection.Stmt2Init((ulong)reqId);
            return new WSStmt(this, resp.StmtId, _tz, connection);
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
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfDisposed();
                Reconnect();
                return DoQuery(query, reqId);
            }
        }

        private IRows DoQuery(string query, long reqId)
        {
            var connection = _connection;
            if (connection == null)
            {
                throw new ObjectDisposedException(nameof(WSClient));
            }

            var resp = connection.BinaryQuery(query, (ulong)reqId);
            if (resp.IsUpdate)
            {
                return new WSRows(resp.AffectedRows);
            }

            return new WSRows(resp.ResultId, resp, connection, _tz);
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
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfDisposed();
                Reconnect();
                return DoExec(query, reqId);
            }
        }

        private long DoExec(string query, long reqId)
        {
            var connection = _connection;
            if (connection == null)
            {
                throw new ObjectDisposedException(nameof(WSClient));
            }

            var resp = connection.BinaryQuery(query, (ulong)reqId);
            if (!resp.IsUpdate)
            {
                connection.FreeResult(resp.ResultId);
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
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfDisposed();
                Reconnect();
                DoSchemalessInsert(lines, protocol, precision, ttl, reqId);
            }
        }

        private void DoSchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            var line = string.Join("\n", lines);
            var connection = _connection;
            if (connection == null)
            {
                throw new ObjectDisposedException(nameof(WSClient));
            }

            connection.SchemalessInsert(line, protocol, precision, ttl, reqId);
        }

        public bool ConnectionAvailable()
        {
            var connection = _connection;
            return connection != null && connection.IsAvailable();
        }

        public Connection TryReconnectOrGetConnection(Connection old)
        {
            ThrowIfDisposed();
            var currentConnection = _connection;
            // first check if the current connection is available
            if (currentConnection != old && currentConnection != null && currentConnection.IsAvailable())
            {
                return currentConnection;
            }

            // force reconnect, new connection must not be old one.
            Reconnect(true, old);
            currentConnection = _connection;
            if (currentConnection != null && currentConnection.IsAvailable())
            {
                return currentConnection;
            }

            throw new TDengineError((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED,
                "websocket connection reconnect failed");
        }
    }
}

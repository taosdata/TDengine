using System;
using System.Diagnostics;
using System.Text;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.Driver.Client.Native
{
    public class NativeClient : ITDengineClient
    {
        private IntPtr _conn;
        private readonly TimeZoneInfo _tz;

        public NativeClient(ConnectionStringBuilder builder)
        {
            Debug.Assert(builder.Protocol == TDengineConstant.ProtocolNative);
            if (!string.IsNullOrEmpty(builder.BearerToken))
            {
                // Use bearer token to connect
                _conn = NativeMethods.ConnectToken(builder.Host, builder.BearerToken, builder.Database,
                    (ushort)builder.Port);
            }
            else
            {
                // Use username and password to connect
                _conn = NativeMethods.Connect(builder.Host, builder.Username, builder.Password, builder.Database,
                    (ushort)builder.Port);
            }

            if (_conn == IntPtr.Zero)
            {
                throw new TDengineError(NativeMethods.ErrorNo(IntPtr.Zero), NativeMethods.Error(IntPtr.Zero));
            }

            _tz = builder.GetTimeZone();
            // set app name
            SetConnectOptions((int)TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP,
                TDengineConstant.ProcessName, "user_app");
            // set connector info
            SetConnectOptions((int)TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_CONNECTOR_INFO,
                TDengineConstant.NativeConnectorInfo, "connector_info");
            if (builder.ConnectionTimezone != null)
            {
                // set timezone
                SetConnectOptions((int)TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_TIMEZONE,
                    builder.ConnectionTimezone.Id, "timezone");
            }
        }

        private const int TSDB_CODE_INVALID_PARA = 0x0118;

        private void SetConnectOptions(int option, string value, string optionName)
        {
            var errNo = NativeMethods.OptionsConnection(_conn, option, value);
            if (errNo == 0) return;
            if ((errNo & 0xffff) == TSDB_CODE_INVALID_PARA)
            {
                // ignore invalid parameter error, because some old version TDengine may not support some options
                return;
            }

            throw new TDengineError(errNo, NativeMethods.Error(IntPtr.Zero),
                $"set connection option {optionName} failed");
        }

        public void Dispose()
        {
            if (_conn == IntPtr.Zero) return;
            NativeMethods.Close(_conn);
            _conn = IntPtr.Zero;
        }

        public IStmt StmtInit()
        {
            return StmtInit(ReqId.GetReqId());
        }

        public IStmt StmtInit(long reqId)
        {
            var stmt = NativeMethods.TaosStmt2Init(_conn, reqId, true, true);
            return new NativeStmt(stmt, _tz);
        }

        public IRows Query(string query)
        {
            return Query(query, ReqId.GetReqId());
        }

        public IRows Query(string query, long reqId)
        {
            var result = NativeMethods.QueryWithReqid(_conn, query, reqId);
            CheckError(result);
            var isUpdate = NativeMethods.IsUpdateQuery(result);
            if (isUpdate == 0) return new NativeRows(result, _tz, false);
            var affectRows = NativeMethods.AffectRows(result);
            NativeMethods.FreeResult(result);
            return new NativeRows(affectRows);
        }

        public long Exec(string query)
        {
            return Exec(query, ReqId.GetReqId());
        }

        public long Exec(string query, long reqId)
        {
            var result = NativeMethods.QueryWithReqid(_conn, query, reqId);
            CheckError(result);
            var affected = NativeMethods.AffectRows(result);
            NativeMethods.FreeResult(result);
            return affected;
        }

        public void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            var line = string.Join("\n", lines);
            byte[] utf8Bytes = Encoding.UTF8.GetBytes(line);
            var result = NativeMethods.SchemalessInsertRawTTLWithReqid(_conn, utf8Bytes, utf8Bytes.Length,
                out _, (int)protocol, (int)precision, ttl, reqId);
            CheckError(result);
            NativeMethods.FreeResult(result);
        }

        private void CheckError(IntPtr result)
        {
            var errNo = NativeMethods.ErrorNo(result);
            if (errNo == 0) return;
            var error = new TDengineError(errNo, NativeMethods.Error(result));
            NativeMethods.FreeResult(result);
            throw error;
        }

        public bool ConnectionAvailable()
        {
            if (_conn == IntPtr.Zero) return false;
            var code = NativeMethods.IsConnectionAlive(_conn);
            return code == 1;
        }
    }
}
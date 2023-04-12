using log4net;
using System;
using IpcDataType = System.String;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDTypeV1Converter
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        internal static IpcDataType ToIpcType(string pointType)
        {
            switch (pointType)
            {
                case "INT":
                    return IpcDataTypes.Int32Type;
                case "INT8":
                    return IpcDataTypes.Int8Type;
                case "INT16":
                    return IpcDataTypes.Int16Type;
                case "INT32":
                    return IpcDataTypes.Int32Type;
                case "INT64":
                    return IpcDataTypes.Int64Type;
                case "NCHAR(100)":
                    return IpcDataTypes.VarCharType;
                case "FLOAT":
                    return IpcDataTypes.Float32Type;
                case "TIMESTAMP":
                    return IpcDataTypes.TimestampType;


                case "Float16":
                    return "FLOAT";
                case "Float32":
                    return "FLOAT";
                case "Float64":
                    return "DOUBLE";
                case "String":
                    return "NCHAR(100)";
                case "Timestamp":
                    return "TIMESTAMP";
            }
            log.Fatal($"PointType not found.{pointType}");
            throw new Exception("PointType not found.");
        }

        internal static Apache.Arrow.Types.IArrowType ToArrowType(string pointType)
        {
            switch (pointType)
            {
                case "INT":
                    return Apache.Arrow.Types.Int32Type.Default;
                case "INT8":
                    return Apache.Arrow.Types.Int8Type.Default;
                case "INT16":
                    return Apache.Arrow.Types.Int16Type.Default;
                case "INT32":
                    return Apache.Arrow.Types.Int32Type.Default;
                case "INT64":
                    return Apache.Arrow.Types.Int64Type.Default;
                case "NCHAR(100)":
                    return Apache.Arrow.Types.StringType.Default;
                case "FLOAT":
                    return Apache.Arrow.Types.FloatType.Default;
                case "TIMESTAMP":
                    return Apache.Arrow.Types.TimestampType.Default;
            }
            log.Fatal($"PointType not found.{pointType}");
            throw new Exception("PointType not found.");
        }
    }

    public static class DateTimeExtensions
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long ToMillisecondsTimestamp(this DateTime dateTime)
        {
            return (long)(dateTime.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
        }
    }
}

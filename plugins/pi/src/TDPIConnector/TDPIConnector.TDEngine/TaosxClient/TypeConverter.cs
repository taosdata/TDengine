using log4net;
using System;
using TDPIConnector.TDEngine.Models;
using IpcDataType = System.String;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDTypeV1Converter
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        /// <summary>
        /// 字符串表示的 TD 类型转换为 TDValueType。
        /// 字符串表示的 TD 类型一定来自 PI 类型转换为 TD 类型的结果，参考：TDPIConnector.Core.Conversions.Converter
        /// </summary>
        internal static TDValueType ToTDType(string tdType)
        {
            switch (tdType)
            {
                case "INT":
                    return TDValueType.Int;
                case "BIGINT":
                    return TDValueType.BigInt;
                case "NCHAR(100)":
                    return TDValueType.String;
                case "FLOAT":
                    return TDValueType.Float;
                case "DOUBLE":
                    return TDValueType.Double;
                case "TIMESTAMP":
                    return TDValueType.Timestamp;
            }
            log.Fatal($"PointType not supported:{tdType}");
            throw new Exception($"Can't convert to TDValueType: {tdType}");
        }

        internal static Apache.Arrow.Types.IArrowType ToArrowType(TDValueType tdType) {
            switch (tdType) { 
                case TDValueType.Int:
                    return Apache.Arrow.Types.Int32Type.Default;
                case TDValueType.BigInt:
                    return Apache.Arrow.Types.Int64Type.Default;
                case TDValueType.String:
                    return Apache.Arrow.Types.StringType.Default;
                case TDValueType.Float:
                    return Apache.Arrow.Types.FloatType.Default;
                case TDValueType.Double:
                    return Apache.Arrow.Types.DoubleType.Default;
                case TDValueType.Timestamp:
                    return Apache.Arrow.Types.TimestampType.Default;
                case TDValueType.Boolean:
                    return Apache.Arrow.Types.BooleanType.Default;
                case TDValueType.None:
                    return Apache.Arrow.Types.NullType.Default;
                default:
                    log.Fatal($"TDValueType not supported:{tdType}");
                    throw new Exception($"Can't convert to ArrowType: {tdType}");

            }
        }

        internal static IpcDataType ToIpcType(TDValueType tdType) {
            switch (tdType) { 
                case TDValueType.Int:
                    return IpcDataTypes.Int32Type;
                case TDValueType.BigInt:
                    return IpcDataTypes.Int64Type;
                case TDValueType.String:
                    return IpcDataTypes.VarCharType;
                case TDValueType.Float:
                    return IpcDataTypes.Float32Type;
                case TDValueType.Double:
                    return IpcDataTypes.Float64Type;
                case TDValueType.Timestamp:
                    return IpcDataTypes.TimestampType;
                case TDValueType.Boolean:
                    return IpcDataTypes.BoolType;
                default:
                    log.Fatal($"TDValueType not supported:{tdType}");
                    throw new Exception($"Can't convert to IpcType: {tdType}");
            }
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

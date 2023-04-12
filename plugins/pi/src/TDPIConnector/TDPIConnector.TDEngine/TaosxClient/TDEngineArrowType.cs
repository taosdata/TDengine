#define CLOUD_LICENSE_ONLY_DISABLED
using Apache.Arrow.Types;
using System;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public static class TDArrowFormat
    {
        public static IArrowType GetArrowDataType(string tdType)
        {
            switch (tdType)
            {
                case "NCHAR(100)":
                case "NCHAR":
                    return StringType.Default;
                case "DOUBLE":
                    return DoubleType.Default;
                case "INT":
                    return Int32Type.Default;
                case "INT8":
                    return Int8Type.Default;
                case "INT16":
                    return Int16Type.Default;
                case "BIGINT":
                    return Int64Type.Default;
                case "FLOAT":
                    return FloatType.Default;
                case "TIMESTAMP":
                    return TimestampType.Default;
                case "BOOL":
                    return BooleanType.Default;
                default:
                    throw new Exception($"AttributeType not found.{tdType}");
            }          
        }     
    }
}

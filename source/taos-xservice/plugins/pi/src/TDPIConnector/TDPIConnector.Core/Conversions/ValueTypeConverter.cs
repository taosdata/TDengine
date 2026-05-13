using System;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Conversions
{
    public class ValueTypeConverter
    {
        internal static TDValueType Convert(TypeCode valueTypeCode)
        {
            switch (valueTypeCode)
            {
                case TypeCode.Single:
                    return TDValueType.Float;
                case TypeCode.Double:
                    return TDValueType.Double;
                case TypeCode.Int16:
                    return TDValueType.Int;
                case TypeCode.Int32:
                    return TDValueType.Int;
                case TypeCode.Int64:
                    return TDValueType.BigInt;
                case TypeCode.Boolean:
                    return TDValueType.Boolean;
                case TypeCode.String:
                    return TDValueType.String;
                case TypeCode.Object:
                    return TDValueType.String;
                case TypeCode.DateTime:
                    return TDValueType.Timestamp;
            }
            return TDValueType.None;
        }
    }
}

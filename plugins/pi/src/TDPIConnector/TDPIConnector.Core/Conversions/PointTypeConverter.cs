using System;

namespace TDPIConnector.Core.Conversions
{
    public class PointTypeConverter
    {
        internal static string Convert(string pointType)
        {
            switch (pointType)
            {
                case "Digital":
                    return "NCHAR(100)";
                case "Int16":
                    return "INT";
                case "Int32":
                    return "INT";
                case "Int64":
                    return "BIGINT";
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
            throw new Exception("PointType not found.");
        }
    }
}

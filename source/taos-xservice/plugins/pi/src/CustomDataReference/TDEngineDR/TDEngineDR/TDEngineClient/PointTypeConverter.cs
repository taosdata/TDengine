using System;

namespace TDEngineDR.TDEngineClient
{
    public class PointTypeConverter
    {
        internal static string Convert(string pointType)
        {
            switch (pointType)
            {
                case "Digital":
                    return "NCHAR";
                case "Int16":
                    return "INT";
                case "Int32":
                    return "INT";
                case "Int64":
                    return "INT";
                case "Float16":
                    return "FLOAT";
                case "Float32":
                    return "DOUBLE";
                case "Float64":
                    return "DOUBLE";
                case "String":
                    return "NCHAR";
                case "Timestamp":
                    return "TIMESTAMP";
            }
            throw new Exception("PointType not found.");
        }
    }
}

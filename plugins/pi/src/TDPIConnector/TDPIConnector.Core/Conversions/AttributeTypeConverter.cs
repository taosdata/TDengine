using System;

namespace TDPIConnector.Core.Conversions
{
    public class AttributeTypeConverter
    {
        internal static string Convert(Type attributeType)
        {
            switch (attributeType.Name)
            {
                case "AFEnumerationValue":
                    return "NCHAR(100)";
                case "Single":
                    return "FLOAT";
                case "Double":
                    return "DOUBLE";
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
                case "DateTime":
                    return "TIMESTAMP";
                case "Guid":
                    return "NCHAR(100)";
                case "Boolean":
                    return "BOOL";
                case "Byte":
                    return "NCHAR(100)";
            }
            throw new Exception("AttributeType not found.");
        }
    }
}

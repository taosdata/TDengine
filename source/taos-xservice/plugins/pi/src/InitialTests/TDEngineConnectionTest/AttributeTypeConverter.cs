using System;
using TDEngineHttpClient.Models;

namespace TDEngineConnectionTest
{
    public class AttributeTypeConverter
    {
        internal static string Convert(Type attributeType)
        {
            switch (attributeType.Name)
            {
                case "Digital":
                    return "NCHAR";               
                case "Single":
                    return "FLOAT";
                case "Double":
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

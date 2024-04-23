using System;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;
namespace TDPIConnector.Core.Conversions
{
    class TableNameConvert
    {
        private static string sigle_element_prefix = "sigle_";

        public static string GetAFPointSuperTableName(AFElementTemplateWrapper template)
        {
            return template.Name;
        }

        public static string GetPITypeStringFromValType(TDValueType ValueType) {
            switch (ValueType)
            {
                case TDValueType.Int:
                    return "INT";
                case TDValueType.Float:
                    return "FLOAT";
                case TDValueType.Double:
                    return "DOUBLE";
                case TDValueType.String:
                    return "NCHAR";
                case TDValueType.Timestamp:
                    return "TIMESTAMP";
            }
            throw new Exception("PointType not found.");
        }
        public static string GetSingleElementSuperTableName(AFElementWrapper element)
        {
            return $"{sigle_element_prefix}{element.Name}";
        }
    }
}

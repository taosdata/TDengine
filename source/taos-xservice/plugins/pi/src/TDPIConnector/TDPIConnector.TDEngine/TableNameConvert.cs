using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Conversions;

namespace TDPIConnector.Core.Conversions
{
    class TableNameConvert
    {
        private static string prefix = "pitag_";
        public static string GetPIPointSuperTableName(PI.PIPointWrapper point) {
            string tdColumnType = PointTypeConverter.Convert(point.PointType);
            string superTableName = $"{prefix}{tdColumnType.Split('(')[0]}";
            return superTableName;
        }

        public static string GetAFPointSuperTableName(AFElementTemplateWrapper template)
        {
            return template.Name;
        }

        public static string GetPIPointSTableNameByTDVType(TDValueType ValueType) {
            string tdColumnType = GetPITypeStringFromValType(ValueType);
            string superTableName = $"{prefix}{tdColumnType.Split('(')[0]}";
            return superTableName;
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
    }
}

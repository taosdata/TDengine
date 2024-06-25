using log4net;
using System;

namespace TDPIConnector.Core.Conversions
{
    /// <summary>
    /// 元素的 value type 转 TDengine 的数据类型
    /// </summary>
    public class AttributeTypeConverter
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        internal static string Convert(string dataReference, Type attributeType)
        {
            if (dataReference == "URI Builder") return "NCHAR(256)";
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
            log.Error($"AttributeType:{attributeType.Name} not supported, please conntact Tdengine");
            return null;
        }
    
    }
}

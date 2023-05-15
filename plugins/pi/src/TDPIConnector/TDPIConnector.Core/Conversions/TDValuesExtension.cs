using log4net;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Conversions
{
    public static class TDValuesExtension
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static TDValue ToTDValue(this AFValueWrapper value)
        {
            if (value.OnMaxTime())
            {
                return null;
            }
            if (value.IsAFEnumerationValue())
            {
                AFEnumerationValueWrapper enumValue = value.GetEnumerationValue();
                if (enumValue.GetEnumerationSetName() == "System")
                {
                    return new TDValue(enumValue.Value, enumValue.Name, value.Timestamp.UtcTime);
                }
                else
                {
                    return new TDValue(enumValue.Name, value.Timestamp.UtcTime, ValueTypeConverter.Convert(value.ValueTypeCode));
                }
            }
            else
            {
                return new TDValue(value.Value, value.Timestamp.UtcTime, ValueTypeConverter.Convert(value.ValueTypeCode));
            }
        }
    }
}

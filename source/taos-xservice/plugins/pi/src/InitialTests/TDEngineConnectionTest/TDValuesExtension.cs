using PISystemWrapper;
using System;
using TDEngineHttpClient.Models;

namespace TDEngineConnectionTest
{
    public static class TDValuesExtension
    {
        public static TDValue ToTDValue(this AFValueWrapper value)
        {
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

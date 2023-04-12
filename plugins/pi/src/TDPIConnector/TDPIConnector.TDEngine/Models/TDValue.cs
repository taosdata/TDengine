using System;
using TDPIConnector.TDEngine.Helper;

namespace TDPIConnector.TDEngine.Models
{
    public class TDValue
    {
        public TDValue(object value, DateTime timestamp, TDValueType valueType)
        {
            this.Value = value;
            this.Timestamp = timestamp;      
            this.ValueType = valueType;
            this.Quality = 0;
        }

        public TDValue(int quality, string status, DateTime timestamp)
        {
            this.Quality = quality;
            this.Status = status;
            this.Timestamp = timestamp;
            this.ValueType = TDValueType.None;
            this.Value = null;
        }

        public TDValue(object value, DateTime timestamp, int quality, TDValueType valueType)
        {
            this.Value = value;
            this.Timestamp = timestamp.ToUniversalTime();
            this.ValueType = valueType;
            this.Quality = quality;
        }

        public string Name { get;  set; }
        public Object Value { get; set; }
        public DateTime Timestamp { get; set; }
        public TDValueType ValueType { get; }
        public int Quality { get; private set; }
        public string Status { get; private set; }
        public string ValueString
        {
            get
            {
                return GetStringValue();
            }
        }
        public string TimestampString
        {
            get
            {
                return this.Timestamp.ToUtcTimeString();
            }
        }
        public string GetStringValue()
        {
            switch (this.ValueType)
            {
                case TDValueType.Double:
                    return Convert.ToDouble(Value).ToString();
                case TDValueType.Float:
                    return Convert.ToSingle(Value).ToString();
                case TDValueType.Int:
                    return Convert.ToInt32(Value).ToString();
                case TDValueType.BigInt:
                    return Convert.ToInt64(Value).ToString();
                case TDValueType.String:
                    string v = Value.ToString();
                    if (v.Length > 100)
                    {
                        v = v.Substring(0, 100);
                    }
                    return $"\'{v}\'";

                case TDValueType.Timestamp:
                    DateTimeOffset dateTimeOffset = new DateTimeOffset((DateTime)Value);
                    return dateTimeOffset.ToUnixTimeMilliseconds().ToString();
                case TDValueType.Boolean:
                    return Convert.ToBoolean(Value).ToString();
            }
            return string.Empty;
        }
    }
}


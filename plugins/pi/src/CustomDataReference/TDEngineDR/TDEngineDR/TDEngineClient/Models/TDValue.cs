using System;
using System.Diagnostics;

namespace TDEngineDR.TDEngineClient.Models
{
    [DebuggerDisplay("Value = {Value}, Timestamp = {Timestamp}, Quality = {Quality}, ValueType = {ValueType}")]
    public class TDValue
    {
        public TDValue(object value, DateTime timestamp, int quality, TDValueType valueType)
        {
            this.Value = value;
            this.Timestamp = timestamp.ToUniversalTime();
            this.ValueType = valueType;
            this.Quality = quality;
        }


        public Object Value { get; set; }
        public DateTime Timestamp { get; set; }
        public TDValueType ValueType { get; }
        public int Quality { get; private set; }
        public string Status { get; private set; }

        public double GetValueAsDouble()
        {
            return Convert.ToDouble(Value);
        }
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
                return this.Timestamp.ToString("yyyy-MM-dd HH:mm:ss");
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
                case TDValueType.String:
                    string v = Value.ToString();
                    if (v.Length > 100)
                    {
                        v = v.Substring(0, 100);
                    }
                    return $"\'{v}\'";

                case TDValueType.Timestamp:
                    return $@"'{((DateTime)Value).ToString("yyyy-MM-dd HH:mm:ss")}'";
                case TDValueType.Boolean:
                    return Convert.ToBoolean(Value).ToString();
            }
            return string.Empty;
        }
    }
}


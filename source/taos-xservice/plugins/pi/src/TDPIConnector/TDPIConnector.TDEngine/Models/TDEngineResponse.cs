using System;
using System.Collections.Generic;
using System.Globalization;

namespace TDPIConnector.TDEngine.Models
{
    public class TDEngineResponse
    {
        public int Code { get; set; }
        public string Desc { get; set; }

        public int Rows { get; set; }

        public List<List<string>> Data { get; set; }

        public List<List<string>> Column_Meta { get; set; }
        public TDEngineResponse()
        {

        }
        internal TDValue ToTDValue()
        {
            TDValueType valueType = GetValueType();
            if (Data.Count == 0)
            {
                return null;
            }
            var dataItem = Data[0];
            int quality = Convert.ToInt32(dataItem[2]);
            DateTime timestamp = DateTime.Parse(dataItem[0]);
            object value = GenerateValue(dataItem[1], valueType);
            return new TDValue(value, timestamp, quality, valueType);
        }

        internal TDValues ToTDValues()
        {
            TDValueType valueType = GetValueType();
            List<TDValue> tdValues = new List<TDValue>();
            foreach (List<string> dataItem in Data)
            {
                int quality = Convert.ToInt32(dataItem[2]);
                DateTime timestamp = DateTime.Parse(dataItem[0]);
                object value = GenerateValue(dataItem[1], valueType);

                tdValues.Add(new TDValue(value, timestamp, quality, valueType));
            }

            return new TDValues(tdValues);
        }
        private object GenerateValue(string value, TDValueType valueType)
        {
            switch (valueType)
            {
                case TDValueType.Double:
                    double result;
                    double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
                    return result;
                case TDValueType.Int:
                    return Convert.ToInt32(value);
                case TDValueType.Float:
                    return Convert.ToSingle(value);
                case TDValueType.String:
                    return Convert.ToString(value);
                case TDValueType.Timestamp:
                    return Convert.ToDateTime(value);
            }
            return null;
        }
        private TDValueType GetValueType()
        {
            string tdValueType = Column_Meta[1][1];
            switch (tdValueType)
            {
                case "INT":
                    return TDValueType.Int;
                case "DOUBLE":
                    return TDValueType.Double;
                case "FLOAT":
                    return TDValueType.Float;
                case "NCHAR":
                    return TDValueType.String;
                case "TIMESTAMP":
                    return TDValueType.Timestamp;
            }

            return TDValueType.None;
        }


    }
}

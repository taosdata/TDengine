using System;

namespace TDEngineDR.TDEngineClient
{
    public class AttributeTypeConverter
    {

        internal static Type Convert(string pointType)
        {
            switch (pointType)
            {
                case "Digital":
                    return typeof(string);
                case "Int16":
                    return typeof(int);
                case "Int32":
                    return typeof(int);
                case "Int64":
                    return typeof(int);
                case "Float16":
                    return typeof(double);
                case "Float32":
                    return typeof(double);
                case "Float64":
                    return typeof(double);
                case "String":
                    return typeof(string);
                case "Timestamp":
                    return typeof(DateTime);
            }
            throw new Exception("PointType not found.");
        }
    }
}


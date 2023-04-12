using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System;
using System.Collections.Generic;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR
{
    internal static class AFTDExtensions
    {
        internal static AFValue ToAFValue(this TDValue tdValue, AFAttribute attribute)
        {
            AFValue afValue = null;
            if (tdValue.Quality == 0)
            {
                afValue = new AFValue(tdValue.Value, tdValue.Timestamp);
            }
            else
            {
                AFEnumerationValue enumValue = AFEnumerationSet.SystemStateSet.GetByValue(tdValue.Quality);
                afValue = new AFValue(enumValue, tdValue.Timestamp, null, AFValueStatus.Bad);
            }
            afValue.Attribute = attribute;
            return afValue;
        }

        internal static AFValues ToAFValues(this TDValues tdValues, AFAttribute attribute)
        {
            AFValues values = new AFValues();
            foreach (TDValue tdValue in tdValues)
            {
                AFValue afValue = tdValue.ToAFValue(attribute);
                values.Add(afValue);
            }
            return values;
        }

        internal static TDValue ToTDValue(this AFValue value)
        {
            if (value.Value is AFEnumerationValue)
            {
                AFEnumerationValue enumValue = (AFEnumerationValue)value.Value;
                if (enumValue.EnumerationSet.Name == "System")
                {
                    return new TDValue(null, value.Timestamp.UtcTime, -enumValue.Value, TDValueType.None);
                }
                else
                {
                    return new TDValue(enumValue.Name, value.Timestamp.UtcTime, 0, ConvertValueType(value.ValueTypeCode));
                }
            }
            else
            {
                return new TDValue(value.Value, value.Timestamp.UtcTime, 0, ConvertValueType(value.ValueTypeCode));
            }
        }

        internal static TDValueType ConvertValueType(TypeCode valueTypeCode)
        {
            switch (valueTypeCode)
            {
                case TypeCode.Single:
                    return TDValueType.Float;
                case TypeCode.Double:
                    return TDValueType.Double;
                case TypeCode.Int16:
                    return TDValueType.Int;
                case TypeCode.Int32:
                    return TDValueType.Int;
                case TypeCode.Int64:
                    return TDValueType.Int;
                case TypeCode.Boolean:
                    return TDValueType.Boolean;
                case TypeCode.String:
                    return TDValueType.String;
                case TypeCode.Object:
                    return TDValueType.String;
                case TypeCode.DateTime:
                    return TDValueType.Timestamp;
                default:
                    return TDValueType.None;

            }
        }


        internal static TDValues ToTDValues(this AFValues values)
        {
            TDValues tdValues = new TDValues();
            foreach (AFValue value in values)
            {
                tdValues.Add(value.ToTDValue());
            }
            return tdValues;
        }

        internal static AFValue ToAFError(this Exception ex)
        {
            return new AFValue(ex.Message);
        }

        internal static IDictionary<AFSummaryTypes, AFValue> ToDicAFValues(this IDictionary<TDSummaryTypes, TDValue> summary, AFAttribute attribute)
        {
            IDictionary<AFSummaryTypes, AFValue> afDic = new Dictionary<AFSummaryTypes, AFValue>();
            foreach (KeyValuePair<TDSummaryTypes, TDValue> keyValuePair in summary)
            {
                AFSummaryTypes summaryTypes = keyValuePair.Key.ToAFSummaryType();
                AFValue afValue = keyValuePair.Value.ToAFValue(attribute);
                afDic.Add(summaryTypes, afValue);
            }
            return afDic;
        }

        internal static IDictionary<AFSummaryTypes, AFValues> ToDicAFValues(this IDictionary<TDSummaryTypes, TDValues> summaries, AFAttribute attribute)
        {
            IDictionary<AFSummaryTypes, AFValues> afDic = new Dictionary<AFSummaryTypes, AFValues>();
            foreach (KeyValuePair<TDSummaryTypes, TDValues> keyValuePair in summaries)
            {
                AFSummaryTypes summaryTypes = keyValuePair.Key.ToAFSummaryType();
                AFValues afValues = keyValuePair.Value.ToAFValues(attribute);
                afDic.Add(summaryTypes, afValues);
            }
            return afDic;
        }

        internal static AFSummaryTypes ToAFSummaryType(this TDSummaryTypes tdSummaryTypes)
        {
            AFSummaryTypes summaryTypes = AFSummaryTypes.None;
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Average))
            {
               summaryTypes = summaryTypes | AFSummaryTypes.Average;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Count))
            {
                summaryTypes = summaryTypes | AFSummaryTypes.Count;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Maximum))
            {
                summaryTypes = summaryTypes | AFSummaryTypes.Maximum;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Minimum))
            {
                summaryTypes = summaryTypes | AFSummaryTypes.Minimum;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.StdDev))
            {
                summaryTypes = summaryTypes | AFSummaryTypes.StdDev;
            }
            if (tdSummaryTypes.HasFlag(TDSummaryTypes.Total))
            {
                summaryTypes = summaryTypes | AFSummaryTypes.Total;
            }
            return summaryTypes;
        }

        internal static TDSummaryTypes ToTDSummaryType(this AFSummaryTypes summaryTypes)
        {
            TDSummaryTypes tdSummaryTypes = TDSummaryTypes.None;
            if (summaryTypes.HasFlag(AFSummaryTypes.Average))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.Average;
            }
            if (summaryTypes.HasFlag(AFSummaryTypes.Count))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.Count;
            }
            if (summaryTypes.HasFlag(AFSummaryTypes.Maximum))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.Maximum;
            }
            if (summaryTypes.HasFlag(AFSummaryTypes.Minimum))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.Minimum;
            }
            if (summaryTypes.HasFlag(AFSummaryTypes.StdDev))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.StdDev;
            }
            if (summaryTypes.HasFlag(AFSummaryTypes.Total))
            {
                tdSummaryTypes = tdSummaryTypes | TDSummaryTypes.Total;
            }
            return tdSummaryTypes;
        }
    }
}

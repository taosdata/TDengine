using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using TDEngineDR.TDEngineClient;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR
{
    [Serializable]
    [Guid("F0453D68-0F71-4B97-ABBE-B4459CF3C2B1")]
    [Description("TDengine; Get TDengine data through HTTP requests.")]
    public class TDEngineDataReference : AFDataReference
    {
        private string configString;



        public TDEngineDataReference() : base()
        {
            PISystem piSystem = this.SafeGetPISystem;
            SimpleLogger.CreateDefaultInstance(piSystem);
        }


        public override AFDataMethods SupportedDataMethods
        {
            get
            {
                return AFDataMethods.DataPipe |
                    AFDataMethods.UpdateValue |
                    AFDataMethods.UpdateValues |
                    AFDataMethods.Asynchronous |
                    AFDataMethods.Future |
                    AFDataMethods.InterpolatedValue |
                    AFDataMethods.InterpolatedValues |
                    AFDataMethods.InterpolatedValuesAtTimes |
                    AFDataMethods.PlotValues |
                    AFDataMethods.RecordedValue |
                    AFDataMethods.RecordedValues |
                    AFDataMethods.RecordedValuesByCount |
                    AFDataMethods.RecordedValuesAtTimes |
                    AFDataMethods.Summaries |
                    AFDataMethods.Summary;
            }
        }

        private TDEngineHttpClient tdEngineClient;



        public override AFDataReferenceContext SupportedContexts
        {
            get
            {
                return AFDataReferenceContext.All;
            }
        }

        public override AFDataReferenceMethod SupportedMethods
        {
            get
            {
                return AFDataReferenceMethod.GetValue |
                    AFDataReferenceMethod.GetValues |
                    AFDataReferenceMethod.SetValue |
                    AFDataReferenceMethod.MultipleAttributes;

            }
        }


        private TDPIStream tdPIStream;
        public TDPIStream TDPIStream
        {
            get
            {
                if (tdPIStream == null)
                {
                    ConfigStringInfo configStringInfo = new ConfigStringInfo(this.configString);
                    this.tdEngineClient = TDEngineServerManager.GetTDEngineClient(configStringInfo.Server, this.PISystem);

                    this.tdPIStream = GetTDPIStream(this.configString, this.PISystem);
                }
                return tdPIStream;
            }
        }

        public override Type EditorType
        {
            get
            {
                return typeof(ConfigStringEditor);
            }
        }

        public override string ConfigString
        {
            get
            {
                return configString;
            }
            set
            {
                if (configString != value)
                {
                    configString = value.Trim();
                    tdPIStream = null;
                    SaveConfigChanges();
                }
            }
        }

        public override AFValue GetValue(object context, object timeContext, AFAttributeList inputAttributes, AFValues inputValues)
        {
            AFTime timestamp;
            if (timeContext is AFTime)
                timestamp = (AFTime)timeContext;
            else if (timeContext is AFTimeRange)
                timestamp = ((AFTimeRange)timeContext).EndTime;
            else
                timestamp = AFTime.Now;

            TDValue tdValue = TDPIStream.GetSnapshotValue();
            if (tdValue == null)
            {
                AFEnumerationValue enumValue = AFEnumerationSet.SystemStateSet.GetByValue(248);
                return new AFValue(enumValue, timestamp, null);
            }
            if (timestamp.UtcTime < tdValue.Timestamp)
            {
                tdValue = TDPIStream.InterpolatedValue(timestamp.UtcTime);
            }
            return tdValue.ToAFValue(this.Attribute);
        }

        public override AFValues GetValues(object context, AFTimeRange timeRange, int numberOfValues, AFAttributeList inputAttributes, AFValues[] inputValues)
        {
            if (numberOfValues > 0)
            {
                return this.PlotValues(timeRange, numberOfValues, null, null, null);
            }
            else if (numberOfValues < 0)
            {
                return this.InterpolatedValuesByCount(timeRange, -numberOfValues, null, false, null, null);
            }
            else
            {
                return this.RecordedValues(timeRange, AFBoundaryType.Inside, null, false, null, null, null, 0);
            }
        }

        public override AFValues InterpolatedValuesByCount(AFTimeRange timeRange, int numberOfValues, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues)
        {
            TDValues tdValues = TDPIStream.PlotValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, numberOfValues);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override async Task<AFValues> InterpolatedValuesByCountAsync(AFTimeRange timeRange, int numberOfValues, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, CancellationToken cancellationToken)
        {
            TDValues tdValues = await TDPIStream.PlotValuesAsync(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, numberOfValues);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override AFValue InterpolatedValue(AFTime time, AFAttributeList inputAttributes, AFValues inputValues)
        {
            TDValue tdValue = TDPIStream.InterpolatedValue(time);
            return tdValue.ToAFValue(this.Attribute);
        }

        public override async Task<AFValue> InterpolatedValueAsync(AFTime time, AFAttributeList inputAttributes, AFValues inputValues, CancellationToken cancellationToken = default)
        {
            TDValue tdValue = await TDPIStream.InterpolatedValueAsync(time);
            return tdValue.ToAFValue(this.Attribute);
        }

        public override AFValues InterpolatedValuesAtTimes(IList<AFTime> times, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues)
        {
            TDValues tdValues = TDPIStream.InterpolatedValuesAtTimes(times.Select(t => t.UtcTime).ToList());
            return tdValues.ToAFValues(this.Attribute);
        }
        public override async Task<AFValues> InterpolatedValuesAtTimesAsync(IList<AFTime> times, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = await TDPIStream.InterpolatedValuesAtTimesAsync(times.Select(t => t.UtcTime).ToList());
            return tdValues.ToAFValues(this.Attribute);
        }


        public override AFValues PlotValues(AFTimeRange timeRange, int intervals, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes)
        {
            TDValues tdValues = TDPIStream.PlotValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, intervals);
            return tdValues.ToAFValues(this.Attribute);
        }
        public override async Task<AFValues> PlotValuesAsync(AFTimeRange timeRange, int intervals, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = await TDPIStream.PlotValuesAsync(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, intervals);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override AFValue RecordedValue(AFTime time, AFRetrievalMode mode, AFAttributeList inputAttributes, AFValues inputValues)
        {
            TDValue tdValue = TDPIStream.GetRecordedValue(time.UtcTime);
            return tdValue.ToAFValue(this.Attribute);
        }

        public override async Task<AFValue> RecordedValueAsync(AFTime time, AFRetrievalMode mode, AFAttributeList inputAttributes, AFValues inputValues, CancellationToken cancellationToken = default)
        {
            TDValue tdValue = await TDPIStream.GetRecordedValueAsync(time.UtcTime);
            return tdValue.ToAFValue(this.Attribute);
        }
        public override AFValues RecordedValues(AFTimeRange timeRange, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes, int maxCount)
        {
            TDValues tdValues = TDPIStream.GetRecordedValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override async Task<AFValues> RecordedValuesAsync(AFTimeRange timeRange, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes, int maxCount, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = await TDPIStream.GetRecordedValuesAsync(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override AFValues RecordedValuesAtTimes(IList<AFTime> times, AFRetrievalMode mode, AFAttributeList inputAttributes, AFValues[] inputValues)
        {
            TDValues tdValues = TDPIStream.RecordedValuesAtTimes(times.Select(t => t.UtcTime).ToList());
            return FillMissingValues(tdValues, times, this.Attribute);

        }

        private static AFValues FillMissingValues(TDValues tdValues, IList<AFTime> times, AFAttribute attribute)
        {
            List<AFValue> values = tdValues.ToAFValues(attribute).ToList();

            foreach (AFTime time in times)
            {
                if (!values.Any(v => v.Timestamp.UtcTime == time.UtcTime))
                {
                    AFEnumerationValue enumValue = AFEnumerationSet.SystemStateSet.GetByValue(253);
                    values.Add(new AFValue(enumValue, time, null));
                }
            }
            values = values.OrderBy(v => v.Timestamp).ToList();
            var afValues = new AFValues();
            foreach (AFValue val in values)
            {
                afValues.Add(val);
            }
            return afValues;
        }

        public override async Task<AFValues> RecordedValuesAtTimesAsync(IList<AFTime> times, AFRetrievalMode mode, AFAttributeList inputAttributes, AFValues[] inputValues, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = await TDPIStream.RecordedValuesAtTimesAsync(times.Select(t => t.UtcTime).ToList());
            return FillMissingValues(tdValues, times, this.Attribute);
        }

        public override AFValues RecordedValuesByCount(AFTime startTime, int count, bool forward, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes)
        {
            TDValues tdValues = TDPIStream.RecordedValuesByCount(startTime.UtcTime, count, forward);
            return tdValues.ToAFValues(this.Attribute);
        }
        public override async Task<AFValues> RecordedValuesByCountAsync(AFTime startTime, int count, bool forward, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, AFAttributeList inputAttributes, AFValues[] inputValues, List<AFTime> inputTimes, CancellationToken cancellationToken)
        {
            TDValues tdValues = await TDPIStream.RecordedValuesByCountAsync(startTime.UtcTime, count, forward);
            return tdValues.ToAFValues(this.Attribute);
        }

        public override void SetValue(object context, AFValue newValue)
        {

        }
        public override IDictionary<AFSummaryTypes, AFValues> Summaries(AFTimeRange timeRange, AFTimeSpan summaryDuration, AFSummaryTypes summaryType, AFCalculationBasis calculationBasis, AFTimestampCalculation timeType)
        {
            IDictionary<TDSummaryTypes, TDValues> summaries = TDPIStream.Summaries(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, summaryDuration.ToTimeSpan(), summaryType.ToTDSummaryType());
            return summaries.ToDicAFValues(this.Attribute);
        }
        public override async Task<IDictionary<AFSummaryTypes, AFValues>> SummariesAsync(AFTimeRange timeRange, AFTimeSpan summaryDuration, AFSummaryTypes summaryType, AFCalculationBasis calculationBasis, AFTimestampCalculation timeType, CancellationToken cancellationToken = default)
        {
            IDictionary<TDSummaryTypes, TDValues> summaries = await TDPIStream.SummariesAsync(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, summaryDuration.ToTimeSpan(), summaryType.ToTDSummaryType());
            return summaries.ToDicAFValues(this.Attribute);
        }
        public override IDictionary<AFSummaryTypes, AFValue> Summary(AFTimeRange timeRange, AFSummaryTypes summaryType, AFCalculationBasis calculationBasis, AFTimestampCalculation timeType)
        {
            IDictionary<TDSummaryTypes, TDValue> summary = TDPIStream.Summary(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, summaryType.ToTDSummaryType());
            return summary.ToDicAFValues(this.Attribute);
        }
        public override async Task<IDictionary<AFSummaryTypes, AFValue>> SummaryAsync(AFTimeRange timeRange, AFSummaryTypes summaryType, AFCalculationBasis calculationBasis, AFTimestampCalculation timeType, CancellationToken cancellationToken = default)
        {
            IDictionary<TDSummaryTypes, TDValue> summary = await TDPIStream.SummaryAsync(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, summaryType.ToTDSummaryType());
            return summary.ToDicAFValues(this.Attribute);
        }

        public override void UpdateValue(AFValue value, AFUpdateOption updateOption, AFBufferOption bufferOption)
        {
            TDValue tdValue = value.ToTDValue();
            TDPIStream.UpdateValue(tdValue);
        }
        public override void UpdateValue(AFValue value, AFUpdateOption updateOption)
        {
            TDValue tdValue = value.ToTDValue();
            TDPIStream.UpdateValue(tdValue);
        }

        public override AFErrors<AFValue> UpdateValues(AFValues values, AFUpdateOption updateOption, AFBufferOption bufferOption)
        {
            TDValues tdValues = values.ToTDValues();
            TDPIStream.UpdateValues(tdValues);
            return null;
        }
        public override AFErrors<AFValue> UpdateValues(AFValues values, AFUpdateOption updateOption)
        {
            TDValues tdValues = values.ToTDValues();
            TDPIStream.UpdateValues(tdValues);
            return null;
        }
        public override async Task<AFErrors<AFValue>> UpdateValuesAsync(AFValues values, AFUpdateOption updateOption, AFBufferOption bufferOption, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = values.ToTDValues();
            await TDPIStream.UpdateValuesAsync(tdValues);
            return null;
        }
        public override async Task<AFErrors<AFValue>> UpdateValuesAsync(AFValues values, AFUpdateOption updateOption, CancellationToken cancellationToken = default)
        {
            TDValues tdValues = values.ToTDValues();
            await TDPIStream.UpdateValuesAsync(tdValues);
            return null;
        }

        private static TDPIStream GetTDPIStream(string configString, PISystem piSystem)
        {
            ConfigStringInfo configStringInfo = new ConfigStringInfo(configString);
            TDEngineHttpClient tdEngineClient = TDEngineServerManager.GetTDEngineClient(configStringInfo.Server, piSystem);
            if (!string.IsNullOrEmpty(configStringInfo.Table) && !string.IsNullOrEmpty(configStringInfo.Column))
            {
                return tdEngineClient.GetTDPIStreamFromTable(configStringInfo.Database, configStringInfo.Table, configStringInfo.Column);
            }
            else if (!string.IsNullOrEmpty(configStringInfo.Element) && !string.IsNullOrEmpty(configStringInfo.Attribute))
            {
                return tdEngineClient.GetTDPIStreamFromAF(configStringInfo.Database, configStringInfo.Element, configStringInfo.Attribute);
            }
            else
            {
                return tdEngineClient.GetTDPIStreamFromPI(configStringInfo.Database, configStringInfo.Point);
            }
        }


        static public new AFValues GetValue(AFAttributeList attributes, object context, object timeContext)
        {
            AFTime timestamp;
            if (timeContext is AFTime)
                timestamp = (AFTime)timeContext;
            else if (timeContext is AFTimeRange)
                timestamp = ((AFTimeRange)timeContext).EndTime;
            else
                timestamp = AFTime.Now;

            AFValues afValues = new AFValues();

            foreach (AFAttribute attribute in attributes)
            {
                AFValue afValue = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValue tdValue = tdPoint.GetSnapshotValue();
                    if (timestamp.UtcTime < tdValue.Timestamp)
                    {
                        tdValue = tdPoint.InterpolatedValue(tdValue.Timestamp);
                    }
                    afValue = tdValue.ToAFValue(attribute);
                }
                catch (Exception ex)
                {
                    afValue = new AFValue(attribute, CreateExceptionMessage(ex), timestamp.UtcTime, null, AFValueStatus.Bad);
                    SimpleLogger.Instance.Error($"GetValue: {CreateExceptionMessage(ex)}");
                }
                afValues.Add(afValue);
            }
            return afValues;
        }


        static public AFValues ListGetValue(IList<AFAttribute> attributeList, AFTime timeContext, int mode)
        {

            AFTime timestamp = timeContext;
            if (timestamp.UtcTime == new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc))
            {
                timestamp = DateTime.UtcNow;
            }

            AFValues afValues = new AFValues();

            foreach (AFAttribute attribute in attributeList)
            {
                AFValue afValue = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValue tdValue = tdPoint.GetSnapshotValue();
                    if (timestamp.UtcTime < tdValue.Timestamp)
                    {
                        tdValue = tdPoint.InterpolatedValue(timestamp.UtcTime);
                    }
                    afValue = tdValue.ToAFValue(attribute);
                }
                catch (Exception ex)
                {
                    afValue = new AFValue(attribute, CreateExceptionMessage(ex), timestamp.UtcTime, null, AFValueStatus.Bad);
                    SimpleLogger.Instance.Error($"ListGetValue: {CreateExceptionMessage(ex)}");
                }
                afValues.Add(afValue);
            }
            return afValues;
        }

        static public async Task<AFValues> ListGetValueAsync(IList<AFAttribute> attributeList, AFTime timeContext, int mode, CancellationToken cancellationToken)
        {

            AFTime timestamp = timeContext;
            if (timestamp.UtcTime == DateTime.MinValue)
            {
                timestamp = DateTime.UtcNow;
            }

            AFValues afValues = new AFValues();

            foreach (AFAttribute attribute in attributeList)
            {
                AFValue afValue = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValue tdValue = tdPoint.GetSnapshotValue();
                    if (timestamp.UtcTime < tdValue.Timestamp)
                    {
                        tdValue = await tdPoint.InterpolatedValueAsync(tdValue.Timestamp);
                    }
                    afValue = tdValue.ToAFValue(attribute);
                }
                catch (Exception ex)
                {
                    afValue = new AFValue(attribute, CreateExceptionMessage(ex), timestamp.UtcTime, null, AFValueStatus.Bad);
                    SimpleLogger.Instance.Error($"ListGetValueAsync: {CreateExceptionMessage(ex)}");
                }
                afValues.Add(afValue);
            }
            return afValues;
        }

        static public IEnumerable<AFValues> ListGetRecordedValues(IList<AFAttribute> attributeList, AFTimeRange timeRange,
            AFBoundaryType boundaryType, string filterExpression,
            bool includeFilteredValues, int maxCount, OSIsoft.AF.PI.PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            if (maxCount == 0)
            {
                maxCount = 50000;
            }
            iterativeFallback = true;

            List<AFValues> afValuesList = new List<AFValues>();
            foreach (AFAttribute attribute in attributeList)
            {
                AFValues afValues = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValues tdValues = tdPoint.GetRecordedValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime);
                    afValues = tdValues.ToAFValues(attribute);
                }
                catch (Exception ex)
                {
                    var afValue = new AFValue(attribute, CreateExceptionMessage(ex), timeRange.EndTime.UtcTime, null, AFValueStatus.Bad);
                    afValues = new AFValues();
                    afValues.Add(afValue);
                    SimpleLogger.Instance.Error($"ListGetRecordedValues: {CreateExceptionMessage(ex)}");
                }
                afValuesList.Add(afValues);
            }
            return afValuesList.AsEnumerable();
        }

        static public IEnumerable<IDictionary<AFSummaryTypes, AFValues>> ListGetSummaries(
            IList<AFAttribute> attributeList, AFTimeRange timeRange, AFTimeSpan summaryDuration,
            AFSummaryTypes summaryType, AFCalculationBasis calculationBasis, AFTimestampCalculation timeType,
            PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            iterativeFallback = true;
            List<IDictionary<AFSummaryTypes, AFValues>> result = new List<IDictionary<AFSummaryTypes, AFValues>>();
            foreach (AFAttribute attribute in attributeList)
            {
                IDictionary<AFSummaryTypes, AFValues> attributeSummaries = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    IDictionary<TDSummaryTypes, TDValues> summaries = tdPoint.Summaries(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, summaryDuration.ToTimeSpan(), summaryType.ToTDSummaryType());
                    attributeSummaries = summaries.ToDicAFValues(attribute);
                    result.Add(attributeSummaries);
                }
                catch (Exception ex)
                {
                    SimpleLogger.Instance.Error($"ListGetSummaries: {CreateExceptionMessage(ex)}");
                }
            }
            return result.AsEnumerable();
        }

        static public IEnumerable<AFValues> ListGetPlotValues(IList<AFAttribute> attributeList, AFTimeRange timeRange,
            int intervals, PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            iterativeFallback = true;
            List<AFValues> afValuesList = new List<AFValues>();
            foreach (AFAttribute attribute in attributeList)
            {
                AFValues afValues = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValues tdValues = tdPoint.PlotValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, intervals);
                    afValues = tdValues.ToAFValues(attribute);
                }
                catch (Exception ex)
                {
                    var afValue = new AFValue(attribute, CreateExceptionMessage(ex), timeRange.EndTime.UtcTime, null, AFValueStatus.Bad);
                    afValues = new AFValues();
                    afValues.Add(afValue);
                    SimpleLogger.Instance.Error($"ListGetPlotValues: {CreateExceptionMessage(ex)}");
                }
                afValuesList.Add(afValues);
            }
            return afValuesList.AsEnumerable();
        }

        static public IEnumerable<AFValues> ListGetInterpolatedValuesByCount(IList<AFAttribute> attributeList, AFTimeRange timeRange, int numberOfValues, string filterExpression,
            bool includeFilteredValues, PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            int secondsInterval = Convert.ToInt32(Math.Round(timeRange.Span.TotalSeconds / (1.0 * numberOfValues)));
            iterativeFallback = true;
            List<AFValues> afValuesList = new List<AFValues>();
            foreach (AFAttribute attribute in attributeList)
            {
                AFValues afValues = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValues tdValues = tdPoint.InterpolatedValues(timeRange.StartTime.UtcTime, timeRange.EndTime.UtcTime, new TimeSpan(0, 0, secondsInterval));
                    afValues = tdValues.ToAFValues(attribute);
                }
                catch (Exception ex)
                {
                    var afValue = new AFValue(attribute, CreateExceptionMessage(ex), timeRange.EndTime.UtcTime, null, AFValueStatus.Bad);
                    afValues = new AFValues();
                    afValues.Add(afValue);
                    SimpleLogger.Instance.Error($"ListGetInterpolatedValuesByCount: {CreateExceptionMessage(ex)}");
                }
                afValuesList.Add(afValues);
            }
            return afValuesList.AsEnumerable();
        }

        static public IEnumerable<AFValues> ListGetInterpolatedValuesAtTimes(IList<AFAttribute> attributeList, IList<AFTime> times,
            string filterExpression, bool includeFilteredValues, PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            iterativeFallback = true;
            IEnumerable<DateTime> dtTimes = times.Select(t => t.UtcTime);
            List<AFValues> afValuesList = new List<AFValues>();
            foreach (AFAttribute attribute in attributeList)
            {
                AFValues afValues = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValues tdValues = tdPoint.InterpolatedValuesAtTimes(dtTimes.ToList());
                    afValues = tdValues.ToAFValues(attribute);
                }
                catch (Exception ex)
                {
                    var afValue = new AFValue(attribute, CreateExceptionMessage(ex), DateTime.UtcNow, null, AFValueStatus.Bad);
                    afValues = new AFValues();
                    afValues.Add(afValue);
                    SimpleLogger.Instance.Error($"ListGetInterpolatedValuesAtTimes: {CreateExceptionMessage(ex)}");
                }
                afValuesList.Add(afValues);
            }
            return afValuesList.AsEnumerable();
        }

        static public IEnumerable<AFValues> ListGetRecordedValuesAtTimes(IList<AFAttribute> attributeList, IList<AFTime> times,
            AFRetrievalMode mode, PIPagingConfiguration pagingConfig, out bool iterativeFallback)
        {
            iterativeFallback = true;
            IEnumerable<DateTime> dtTimes = times.Select(t => t.UtcTime);
            List<AFValues> afValuesList = new List<AFValues>();
            foreach (AFAttribute attribute in attributeList)
            {
                AFValues afValues = null;
                try
                {
                    TDPIStream tdPoint = GetTDPIStream(attribute.ConfigString, attribute.PISystem);
                    TDValues tdValues = tdPoint.RecordedValuesAtTimes(dtTimes.ToList());
                    afValues = FillMissingValues(tdValues, times, attribute);
                }
                catch (Exception ex)
                {
                    var afValue = new AFValue(attribute, CreateExceptionMessage(ex), DateTime.UtcNow, null, AFValueStatus.Bad);
                    afValues = new AFValues();
                    afValues.Add(afValue);
                    SimpleLogger.Instance.Error($"ListGetRecordedValuesAtTimes: {CreateExceptionMessage(ex)}");
                }
                afValuesList.Add(afValues);
            }
            return afValuesList.AsEnumerable();
        }

        private static string CreateExceptionMessage(Exception ex)
        {
            Stack<string> stack = new Stack<string>();

            stack.Push(ex.Message);
            if (ex.InnerException != null)
            {
                ex = ex.InnerException;
                stack.Push(ex.Message);
            }

            string message = string.Empty;
            while (stack.Count > 0)
            {
                string msg = stack.Pop();
                message += msg;
            }
            return message;
        }

        // Return an AFEventSource object for this custom data reference
        public static AFEventSource CreateDataPipe()
        {
            TDEventSource pipe = new TDEventSource();
            return pipe;
        }
    }
}

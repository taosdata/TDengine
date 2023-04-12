using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;
using OSIsoft.AF.UnitsOfMeasure;
using System;
using System.Collections.Generic;

namespace ManualTests
{
    class Program
    {
        static void Main()
        {
            //AFAttribute piCdt158Attribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\PIPointDR|Cdt158") as AFAttribute;
            //AFAttribute piSinusoidAttribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\PIPointDR|Sinusoid") as AFAttribute;
            //AFAttribute tdCdt158Attribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\TDEngineDR|Cdt158") as AFAttribute;
            //AFAttribute tdSinusoidAttribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\TDEngineDR|Sinusoid") as AFAttribute;
            ////tdCdt158Attribute.ConfigString = "ServerName=MyLocalServer;PointName=cdt158;";
            ////tdCdt158Attribute.PISystem.CheckIn();

            //AFTime startTime = new AFTime(DateTime.Today.AddDays(-100));
            //AFTime endTime = new AFTime(DateTime.Today.AddDays(-99));
            //AFTimeRange timeRange = new AFTimeRange(startTime, endTime);

            //TimeSpan interval = new TimeSpan(0, 0, 10, 0);
            //AFTimeSpan afInterval = new AFTimeSpan(interval);

            //AFRetrievalMode retrievalMode = AFRetrievalMode.Exact;
            //AFBoundaryType boundaryType = AFBoundaryType.Inside;
            //UOM desiredUOM = null;
            //AFSummaryTypes summaryTypes = AFSummaryTypes.Average | AFSummaryTypes.Count | AFSummaryTypes.Maximum | AFSummaryTypes.Minimum | AFSummaryTypes.Total | AFSummaryTypes.StdDev;

            //IList<AFTime> afTimes = new List<AFTime>()
            //{
            //    startTime.UtcTime,
            //    startTime.UtcTime.AddHours(1),
            //    startTime.UtcTime.AddHours(2),
            //    startTime.UtcTime.AddHours(3),
            //};

            //Console.WriteLine("GetValue");
            //var val = piCdt158Attribute.GetValue();
            //var val2 = tdCdt158Attribute.GetValue();

            //Console.WriteLine("Summary");
            //IDictionary<AFSummaryTypes, AFValue> summary = piCdt158Attribute.Data.Summary(timeRange, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto);
            //IDictionary<AFSummaryTypes, AFValue> summary2 = tdCdt158Attribute.Data.Summary(timeRange, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto);

            //Console.WriteLine("Summaries");
            //IDictionary<AFSummaryTypes, AFValues> summaries = piCdt158Attribute.Data.Summaries(timeRange, afInterval, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto);
            //IDictionary<AFSummaryTypes, AFValues> summaries2 = tdCdt158Attribute.Data.Summaries(timeRange, afInterval, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto);

            //Console.WriteLine("RecordedValues");
            //AFValues recordedValues = piCdt158Attribute.Data.RecordedValues(timeRange, boundaryType, desiredUOM, string.Empty, true, 0);
            //AFValues recordedValues2 = tdCdt158Attribute.Data.RecordedValues(timeRange, boundaryType, desiredUOM, string.Empty, true, 0);

            //Console.WriteLine("RecordedValue");
            //AFValue recordedValue = piCdt158Attribute.Data.RecordedValue(recordedValues[0].Timestamp, retrievalMode, desiredUOM);
            //AFValue recordedValue2 = tdCdt158Attribute.Data.RecordedValue(recordedValues[0].Timestamp, retrievalMode, desiredUOM);




            //Console.WriteLine("RecordedValuesAtTimes");
            //AFValues recordedValues2d = piCdt158Attribute.Data.RecordedValuesAtTimes(afTimes, retrievalMode, desiredUOM);
            //AFValues recordedValues2e = tdCdt158Attribute.Data.RecordedValuesAtTimes(afTimes, retrievalMode, desiredUOM);

            //Console.WriteLine("RecordedValuesByCount");
            //AFValues recordedValues3 = piCdt158Attribute.Data.RecordedValuesByCount(startTime, 1000, true, boundaryType, desiredUOM, string.Empty, true);
            //AFValues recordedValues3a = tdCdt158Attribute.Data.RecordedValuesByCount(startTime, 1000, true, boundaryType, desiredUOM, string.Empty, true);

            //Console.WriteLine("InterpolatedValues");
            //AFValues interpolatedValues1 = piCdt158Attribute.Data.InterpolatedValues(timeRange, afInterval, desiredUOM, string.Empty, true);
            //AFValues interpolatedValues1a = tdCdt158Attribute.Data.InterpolatedValues(timeRange, afInterval, desiredUOM, string.Empty, true);

            //Console.WriteLine("InterpolatedValue");
            //AFValue interpolatedValue = piCdt158Attribute.Data.InterpolatedValue(afTimes[2], desiredUOM);
            //AFValue interpolatedValue1b = tdCdt158Attribute.Data.InterpolatedValue(afTimes[2], desiredUOM);


            //Console.WriteLine("InterpolatedValuesAtTimes");
            //AFValues interpolatedValues2 = piCdt158Attribute.Data.InterpolatedValuesAtTimes(afTimes, desiredUOM, string.Empty, true);
            //AFValues interpolatedValues2a = tdCdt158Attribute.Data.InterpolatedValuesAtTimes(afTimes, desiredUOM, string.Empty, true);

            //Console.WriteLine("PlotValues");
            //AFValues plotValues = piCdt158Attribute.Data.PlotValues(timeRange, 10, desiredUOM);
            //AFValues plotValues2 = tdCdt158Attribute.Data.PlotValues(timeRange, 10, desiredUOM);


            //AFAttributeList piAttributeList = new AFAttributeList();
            //piAttributeList.Add(piCdt158Attribute);
            //piAttributeList.Add(piSinusoidAttribute);

            //AFAttributeList tdAttributeList = new AFAttributeList();
            //tdAttributeList.Add(tdCdt158Attribute);
            ////tdAttributeList.Add(tdSinusoidAttribute);

            //Console.WriteLine("List RecordedValue");
            //var pagingConfig = new PIPagingConfiguration(PIPageType.EventCount, 1000);
            //IEnumerable<AFValue> piRecordedValueList1 = piAttributeList.Data.RecordedValue(recordedValues[0].Timestamp, retrievalMode);
            //IEnumerable<AFValue> tdRecordedValueList1 = tdAttributeList.Data.RecordedValue(recordedValues[0].Timestamp, retrievalMode);

            //Console.WriteLine("List RecordedValues");
            //IEnumerable<AFValues> piRecordedValuesList2 = piAttributeList.Data.RecordedValues(timeRange, boundaryType, string.Empty, false, pagingConfig, 0);
            //IEnumerable<AFValues> tdRecordedValuesList2 = tdAttributeList.Data.RecordedValues(timeRange, boundaryType, string.Empty, false, pagingConfig, 0);

            //Console.WriteLine("List RecordedValuesAtTimes");
            //IEnumerable<AFValues> piRecordedValuesList3 = piAttributeList.Data.RecordedValuesAtTimes(afTimes, retrievalMode, pagingConfig);
            //IEnumerable<AFValues> tdRecordedValuesList3 = tdAttributeList.Data.RecordedValuesAtTimes(afTimes, retrievalMode, pagingConfig);

            //Console.WriteLine("List RecordedValuesByCount");
            //IEnumerable<AFValues> piRecordedValuesList4 = piAttributeList.Data.RecordedValuesByCount(startTime, 1000, true, boundaryType, string.Empty, false, pagingConfig);
            //IEnumerable<AFValues> tdRecordedValuesList4 = tdAttributeList.Data.RecordedValuesByCount(startTime, 1000, true, boundaryType, string.Empty, false, pagingConfig);

            //Console.WriteLine("List InterpolatedValue");
            //IEnumerable<AFValue> piInterpolatedValueList1 = piAttributeList.Data.InterpolatedValue(recordedValues[0].Timestamp);
            //IEnumerable<AFValue> tdInterpolatedValueList1 = tdAttributeList.Data.InterpolatedValue(recordedValues[0].Timestamp);

            //Console.WriteLine("List InterpolatedValues");
            //IEnumerable<AFValues> piInterpolatedValuesList1 = piAttributeList.Data.InterpolatedValues(timeRange, afInterval, string.Empty, false, pagingConfig);
            //IEnumerable<AFValues> tdInterpolatedValuesList1 = tdAttributeList.Data.InterpolatedValues(timeRange, afInterval, string.Empty, false, pagingConfig);

            //Console.WriteLine("List PlotValues");
            //IEnumerable<AFValues> piPlotValuesList1 = piAttributeList.Data.PlotValues(timeRange, 1000, pagingConfig);
            //IEnumerable<AFValues> tdPlotValuesList1 = tdAttributeList.Data.PlotValues(timeRange, 1000, pagingConfig);

            //Console.WriteLine("List Summary");
            //IEnumerable<IDictionary<AFSummaryTypes, AFValue>> piSummaryList = piAttributeList.Data.Summary(timeRange, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto, pagingConfig);
            //IEnumerable<IDictionary<AFSummaryTypes, AFValue>> tdSummaryList = tdAttributeList.Data.Summary(timeRange, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto, pagingConfig);

            //Console.WriteLine("List Summaries");
            //IEnumerable<IDictionary<AFSummaryTypes, AFValues>> piSummariesList = piAttributeList.Data.Summaries(timeRange, afInterval, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto, pagingConfig);
            //IEnumerable<IDictionary<AFSummaryTypes, AFValues>> tdSummariesList = tdAttributeList.Data.Summaries(timeRange, afInterval, summaryTypes, AFCalculationBasis.EventWeighted, AFTimestampCalculation.Auto, pagingConfig);

            Console.WriteLine("AF DataPipe");
            //AFAttribute piAttribute = AFObject.FindObject(@"\\MARC-PI2018\Meters\California\Los Angeles\Meter_13204|Current") as AFAttribute;
            //AFAttribute tdttribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\Meter_13204|Current") as AFAttribute;
            AFAttribute piAttribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\PIPointDR|Cdt158") as AFAttribute;
            AFAttribute tdttribute = AFObject.FindObject(@"\\MARC-PI2018\TDEngineTests\TDEngineDR|Cdt158") as AFAttribute;

            SignUpForUpdates(piAttribute, tdttribute);
            Console.WriteLine("Finished");
            Console.ReadKey();


        }

        private static void SignUpForUpdates(AFAttribute piAttribute, AFAttribute afAttribute)
        {

            AFDataPipe afDataPipe = new AFDataPipe();
            IObserver<AFDataPipeEvent> observer = new AFDataPipeEventObserver();
            afDataPipe.Subscribe(observer);

            IList<AFAttribute> attributes = new List<AFAttribute>()
            {
                piAttribute, afAttribute
            };
            AFErrors<AFAttribute> errors = afDataPipe.AddSignups(attributes);
            while(true)
            {
                afDataPipe.GetObserverEvents();
                System.Threading.Thread.Sleep(5000);
            }
        }
    }
}

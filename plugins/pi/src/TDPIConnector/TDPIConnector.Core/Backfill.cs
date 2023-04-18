using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Conversions;
using log4net;

namespace TDPIConnector.Core
{
    public class Backfill
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private TDEngineProxy tdEngineProxy;
        private PIServerManager piServerManager;
        private PISystemManager piSystemManager;

        public Backfill(TDEngineProxy tdEngineProxy, PIServerManager piServerManager, PISystemManager piSystemManager)
        {
            this.tdEngineProxy = tdEngineProxy;
            this.piServerManager = piServerManager;
            this.piSystemManager = piSystemManager;
        }


        public async Task BackfillPIPointsFromLastRecordedValue(string tdDatabaseName, Dictionary<PIPointWrapper, DateTime> lastValueTimestamps)
        {
            Stopwatch stopwatch = new Stopwatch();
            foreach (var lastTDValue in lastValueTimestamps)
            {
                stopwatch.Start();
                PIPointWrapper piPoint = lastTDValue.Key;
                List<AFValueWrapper> afValues = await PISystemManager.GetPIPointRecordedValuesByCountForward(piPoint, lastTDValue.Value, 5000);
                log.Info($"Backfill PI point {piPoint.Name}, {afValues.Count} values retrieved in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
                stopwatch.Start();
                List<TDValue> tdValues = afValues.Select(afValue => afValue.ToTDValue()).ToList();
                tdEngineProxy.InsertBackfillValuesForPI(tdDatabaseName,TableNameConvert.GetPIPointSuperTableName(piPoint),  piPoint.Name, tdValues);
                log.Info($"Backfill TDEngine point {piPoint.Name}, {tdValues.Count} values written in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
            }
        }


        public async Task BackfillPIPointsToFirstRecordedValue(string tdDatabaseName, Dictionary<string, DateTime> firstValueTimestamps)
        {
            Stopwatch stopwatch = new Stopwatch();

            foreach (var lastTDValue in firstValueTimestamps)
            {
                stopwatch.Start();
                PIPointWrapper piPoint = piServerManager.FindPIPoint(lastTDValue.Key);
                List<AFValueWrapper> afValues = await PISystemManager.GetPIPointRecordedValuesByCountReverse(piPoint, lastTDValue.Value, 5000);
                log.Info($"Backfill PI point {piPoint.Name}, {afValues.Count} values retrieved in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
                stopwatch.Start();
                List<TDValue> tdValues = afValues.Select(afValue => afValue.ToTDValue()).ToList();
                tdEngineProxy.InsertBackfillValuesForPI(tdDatabaseName,TableNameConvert.GetPIPointSuperTableName(piPoint), piPoint.Name, tdValues);
                log.Info($"Backfill TDEngine point {piPoint.Name}, {tdValues.Count} values written in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
            }
        }

        public void BackfillPIPoints(string tdDatabaseName, DateTime startTime, DateTime endTime, List<TDTable> points)
        {
            if (points == null || points.Count == 0)
            {
                return;
            }
            List<string> piPointNames = points.Select(p => p.Name).ToList();
            List<PIPointWrapper> piPoints = piServerManager.FindPIPoints(piPointNames);
            Stopwatch stopwatch = new Stopwatch();

            foreach (var point in piPoints)
            {
                stopwatch.Start();
                List<AFValueWrapper> afValues = PISystemManager.GetPIPointRecordedValues(point, startTime, endTime, 5000);
                log.Info($"PI point {point.Name}, {afValues.Count} values retrived in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
                stopwatch.Start();
                List<TDValue> tdValues = afValues.Select(afValue => afValue.ToTDValue()).ToList();
                string supetableName = TableNameConvert.GetPIPointSuperTableName(point);
                tdEngineProxy.InsertBackfillValuesForPI(tdDatabaseName, supetableName, point.Name, tdValues);
                log.Info($"TDEngine point {point.Name}, {tdValues.Count} values written in {stopwatch.ElapsedMilliseconds} ms");
                stopwatch.Reset();
            }
        }

        public async Task<Dictionary<string, DateTime>> GetTDTableLastRecordedValueFromPIPoints(string tdDatabaseName, List<TDTable> piPointTables)
        {
            List<string> piPointNames = piPointTables.Select(p => p.Name).ToList(); ;
            List<string> STableNames = piPointTables.Select(p => p.STableName).Distinct().ToList();
            return await tdEngineProxy.GetLastPIValues(tdDatabaseName, piPointNames, STableNames);
        }

        public async Task<Dictionary<string, DateTime>> GetTDTableLastRecordedValueFromAFElements(string tdDatabaseName, List<string> elements, IEnumerable<string> elementTemplateNames)
        {
            return await tdEngineProxy.GetLastPIValues(tdDatabaseName, elements, elementTemplateNames);
        }

        public async Task<Dictionary<string, DateTime>> GetTDPointsFirstRecordedValue(string tdDatabaseName, List<string> elements)
        {
            Dictionary<string, DateTime> firstValueTimestamps = new Dictionary<string, DateTime>();
            foreach (var element in elements)
            {
                var firstTDValue = await tdEngineProxy.GetFirstPIValue(tdDatabaseName, element);
                if (firstTDValue == null)
                {
                    firstValueTimestamps.Add(element, DateTime.MaxValue);
                }
                else
                {
                    firstValueTimestamps.Add(element, firstTDValue.Timestamp);
                }
            }
            return firstValueTimestamps;
        }

        public async Task<Dictionary<string, DateTime>> GetTDPointsFirstRecordedValueFromPIPoints(string tdDatabaseName, List<TDTable> points)
        {
            List<string> piPointNames = points.Select(p => p.Name).ToList();
            return await GetTDPointsFirstRecordedValue(tdDatabaseName, piPointNames);
        }

        public void BackfillAFElementsFromLastRecordedValue(string tdDatabaseName, Dictionary<AFElementWrapper, DateTime> lastValueTimestamps)
        {
            foreach (var lastTDValue in lastValueTimestamps)
            {
                AFElementWrapper element = lastTDValue.Key;
                BackfillElement(tdDatabaseName, element, lastTDValue.Value, DateTime.Now);
            }
        }

        internal void BackfillElements(string tdDatabaseName, List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
        {
            foreach (var element in elements)
            {
                BackfillElement(tdDatabaseName, element, startTime, endTime);
            }
        }

        internal void BackfillElement(string tdDatabaseName, AFElementWrapper element, DateTime startTime, DateTime endTime)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            AFAttributeListWrapper attributes = new AFAttributeListWrapper();
            attributes.AddRange(element.Attributes);

            IEnumerable<AFValuesWrapper> valuesList = piSystemManager.GetAttributesRecordedValues(attributes, startTime, endTime, 5000);
            log.Debug($"Backfill AFElement {element.Name}, values retrieved in {stopwatch.ElapsedMilliseconds} ms");
            stopwatch.Reset();
            stopwatch.Start();
            foreach (AFValuesWrapper values in valuesList)
            {
                if (values.Count > 0)
                {
                    AFAttributeWrapper attribute = values[0].Attribute;
                    var superTableName = TableNameConvert.GetAFPointSuperTableName(attribute.Element.Template);
                    ConvertAFAttibutesAndValuesToTDTables(attribute, values, out Dictionary<string, Dictionary<string, List<TDValue>>> tables, out List<string> columnNames);
                    var stables = new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
                    stables.Add(superTableName, tables);
                    this.tdEngineProxy.InsertValuesForAFElements(tdDatabaseName, stables, columnNames).Wait();
                    log.Info($"Backfill TDEngine attribute {element.Name}\\{attribute.Name}, {values.Count} values written in {stopwatch.ElapsedMilliseconds} ms");
                }
            }
            stopwatch.Reset();
        }

        private void ConvertAFAttibutesAndValuesToTDTables(AFAttributeWrapper attribute, AFValuesWrapper values, out Dictionary<string, Dictionary<string, List<TDValue>>> tables, out List<string> columnNames)
        {
            tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();
            columnNames = new List<string>();

            var elementName = attribute.Element.Name;
            if (!columnNames.Contains(attribute.Name))
            {
                columnNames.Add(attribute.Name);
            }
            for (int i = 0; i < values.Count; i++)
            {
                var value = values[i];
                var tdValue = value.ToTDValue();
                var timestamp = tdValue.TimestampString;
                tdValue.Name = attribute.Name;


                if (tables.ContainsKey(elementName))
                {
                    var table = tables[elementName];
                    if (table.ContainsKey(timestamp))
                    {
                        table[timestamp].Add(tdValue);
                    }
                    else
                    {
                        table.Add(timestamp, new List<TDValue>() { tdValue });
                    }
                }
                else
                {
                    tables.Add(elementName, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                }
            }
        }
    }
}

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


        public void BackfillPIPointsFromLastRecordedValue(string tdDatabaseName, Dictionary<PIPointWrapper, DateTime> lastValueTimestamps, DateTime endTime)
        {
            int all = lastValueTimestamps.Count;
            int finished = 0;
            foreach (var lastTDValue in lastValueTimestamps)
            {
                PIPointWrapper piPoint = lastTDValue.Key;
                DateTime startTime = lastTDValue.Value.AddMilliseconds(1);
                BackfillPIPoint(tdDatabaseName, startTime, endTime, piPoint);
                finished++;
                log.Info($"Backfill BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
            }
            log.Info($"Backfill BackfillPIPointsFromLastRecordedValue finished.");
        }

        public void BackfillPIPoints(string tdDatabaseName, DateTime startTime, DateTime endTime, List<TDTable> points)
        {
            if (points == null || points.Count == 0)
            {
                return;
            }
            List<string> piPointNames = points.Select(p => p.Name).ToList();
            List<PIPointWrapper> piPoints = piServerManager.FindPIPoints(piPointNames);
            int all = piPoints.Count;
            int finished = 0;
            foreach (var point in piPoints)
            {
                BackfillPIPoint(tdDatabaseName, startTime, endTime, point);
                finished++;
                log.Info($"Backfill BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
            }
            log.Info($"Backfill BackfillPIPoints finished.");
        }

        public void BackfillPIPoint(string tdDatabaseName, DateTime startTime, DateTime endTime, PIPointWrapper point)
        {
            Stopwatch stopwatch = new Stopwatch();
            int count = 0;
            DateTime pointStartTime = startTime;
            string supetableName = TableNameConvert.GetPIPointSuperTableName(point);
            while (pointStartTime != DateTime.MaxValue)
            {
                stopwatch.Reset();
                stopwatch.Start();
                List<AFValueWrapper> afValues = PISystemManager.GetPIPointRecordedValues(point, ref pointStartTime, endTime, 50000);
                if (afValues.Count == 0) break;
                log.Info($"PI point {point.Name}, {afValues.Count} values got in {stopwatch.ElapsedMilliseconds} ms");
                List<TDValue> tdValues = afValues.Select(afValue => afValue.ToTDValue()).Where(v => v != null).ToList();
                stopwatch.Reset();
                stopwatch.Start();
                log.Info($"PI point {point.Name}, {afValues.Count} values saved in {stopwatch.ElapsedMilliseconds} ms");
                tdEngineProxy.InsertBackfillValuesForPI(tdDatabaseName, supetableName, point.Name, tdValues);
                count += tdValues.Count;
                if (tdValues.Count < 10)
                {
                    break;
                }
            }
            log.Info($"Backfill TDEngine point {point.Name} finished, {count} values written.");
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
                if (tdValue == null) continue;
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

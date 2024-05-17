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
using System.Threading;

namespace TDPIConnector.Core
{
    public class Backfill
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private TDEngineProxy tdEngineProxy;
        private PIServerManager piServerManager;
        private PISystemManager piSystemManager;
        ElementsBackfillTaskManager elemmentBackfillManager = new ElementsBackfillTaskManager();
        int backfillWait = 0;  // ms
        List<Task> backFillTasks = new List<Task>();
        bool stopAddNewTask = false;

        private string tdDatabaseName;

        private class ElementBackfillTask
        {
            public AFElementWrapper element;
            public DateTime startTime;
            public DateTime endTime;

            public ElementBackfillTask(in AFElementWrapper element, DateTime startTime, DateTime endTime)
            {
                this.element = element;
                this.startTime = startTime;
                this.endTime = endTime;
            }
        }
        private class BackfillTask {
            public List<AFElementWrapper> elements;
            public DateTime startTime;
            public DateTime endTime;

            public BackfillTask(in List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
            {
                this.elements = elements;
                this.startTime = startTime;
                this.endTime = endTime;
            }
        }
        private class ElementsBackfillTaskManager {
            List<BackfillTask> elementsTasks = new List<BackfillTask>();
            int currentBatchIndex = 0;
            int currentIndexInBatch = 0;
            int started = 0;
            int finished = 0;
            int all = 0;
            private readonly Object taskLock = new Object();

            public void AddNewElementsTask(List<AFElementWrapper> elements, DateTime startTime, DateTime endTime) {
                lock (taskLock) {
                    elementsTasks.Add(new BackfillTask(elements, startTime, endTime));
                    all += elements.Count();
                }
            }
            public ElementBackfillTask GetNextTask()
            {
                lock (taskLock)
                {
                    if (currentBatchIndex >= elementsTasks.Count()) return null;
                    int nextBatchIndex = currentBatchIndex;
                    int nextIndexInBatch = currentIndexInBatch + 1;
                    if (nextIndexInBatch == elementsTasks[currentBatchIndex].elements.Count()) {
                        nextBatchIndex += 1;
                        nextIndexInBatch = 0;
                    }
                    ++started;
                    log.Info($"backfill element {elementsTasks[currentBatchIndex].elements[currentIndexInBatch].Name}:" +
                        $"{elementsTasks[currentBatchIndex].elements[currentIndexInBatch].ID} startting: {started}/{all}");
                    ElementBackfillTask task = new ElementBackfillTask(elementsTasks[currentBatchIndex].elements[currentIndexInBatch],
                        elementsTasks[currentBatchIndex].startTime, elementsTasks[currentBatchIndex].endTime);
                    currentBatchIndex = nextBatchIndex;
                    currentIndexInBatch = nextIndexInBatch;
                    return task;
                }
            }

            public void FinishedOne() {
                lock (taskLock)
                {
                    ++finished;
                    log.Info($"backfill element {elementsTasks[currentBatchIndex].elements[currentIndexInBatch].Name}:" +
                        $"{elementsTasks[currentBatchIndex].elements[currentIndexInBatch].ID} finshed: {finished}/{all}");
                }
            }
        }
        public Backfill(TDEngineProxy tdEngineProxy, PIServerManager piServerManager, PISystemManager piSystemManager)
        {
            this.tdEngineProxy = tdEngineProxy;
            this.piServerManager = piServerManager;
            this.piSystemManager = piSystemManager;

            string taosPiBackfillWait = Environment.GetEnvironmentVariable("TAOSXPIBACKFILLWAIT");
            if (!int.TryParse(taosPiBackfillWait, out backfillWait))
            {
                backfillWait = 10;
            }
            log.Info($"TAOSXPIBACKFILLWAIT set to {backfillWait}.");
            StartAsyncBackTask();
            Console.WriteLine("Asynchronous task starting...");
        }

        public void StartAsyncBackTask()
        {
            for (int i = 0; i < AppSettings.tomlConfig.BackfillConcurrencyCounts; ++i)
            {
                backFillTasks.Add(Task.Run(async () =>
                {
                    while (true)
                    {
                        var task = elemmentBackfillManager.GetNextTask();
                        if (task != null)
                        {
                            BackfillElement(tdDatabaseName, task.element, task.startTime, task.endTime);
                            elemmentBackfillManager.FinishedOne();
                        }
                        else
                        {
                            if (stopAddNewTask) {
                                return;
                            }
                            Thread.Sleep(1000);
                        }
                    }
                }));
            }
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
            string supetableName = PIInfoScanner.GeneratePointSuperTableName(point);
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

        internal void WaitTask()
        {
            stopAddNewTask = true;
            Task.WaitAll(backFillTasks.ToArray());
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
        public async Task<Dictionary<string, DateTime>> GetTDPointsLastRecordedValue(string tdDatabaseName, List<string> elements)
        {
            Dictionary<string, DateTime> lastValueTimestamps = new Dictionary<string, DateTime>();
            foreach (var element in elements)
            {
                var lastTDValue = await tdEngineProxy.GetLastPIValue(tdDatabaseName, element);
                if (lastTDValue == null)
                {
                    lastValueTimestamps.Add(element, DateTime.MaxValue);
                }
                else
                {
                    lastValueTimestamps.Add(element, lastTDValue.Timestamp);
                }
            }
            return lastValueTimestamps;
        }

        public async Task<Dictionary<string, DateTime>> GetTDPointsFirstRecordedValueFromPIPoints(string tdDatabaseName, List<TDTable> points)
        {
            List<string> piPointNames = points.Select(p => p.Name).ToList();
            return await GetTDPointsFirstRecordedValue(tdDatabaseName, piPointNames);
        }

        public async void BackfillAFElementsFromLastRecordedValue(string tdDatabaseName, Dictionary<AFElementWrapper, DateTime> lastValueTimestamps)
        {
            List<Task> tasks = new List<Task>();
            SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(10);
            foreach (var lastTDValue in lastValueTimestamps)
            {
                await concurrencySemaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    AFElementWrapper element = lastTDValue.Key;
                    BackfillElement(tdDatabaseName, element, lastTDValue.Value, DateTime.Now);
                    concurrencySemaphore.Release();
                }));
            }
            Task.WaitAll(tasks.ToArray());
        }

        internal void BackfillElements(string tdDatabaseName, List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
        {
            elemmentBackfillManager.AddNewElementsTask(elements, startTime, endTime); 
        }

        internal void BackfillElement(string tdDatabaseName, AFElementWrapper element, DateTime startTime, DateTime endTime)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            AFAttributeListWrapper attributes = new AFAttributeListWrapper();
            foreach (AFAttributeWrapper attribute in element.Attributes) {
                if (!attribute.IsTDengineTag() && !attribute.Unsupported()) // tag update is not here
                {
                    attributes.Add(attribute);
                }
            }

            var currentStart = startTime;
            do
            {
                stopwatch.Reset();
                stopwatch.Start();
                IEnumerable<AFValuesWrapper> valuesList = piSystemManager.GetAttributesRecordedValues(attributes, currentStart, endTime, AppSettings.tomlConfig.BackfillBatchSize);
                bool found = false;
                DateTime smallLastAttributeTime = endTime;
                int count = 0;
                foreach (AFValuesWrapper values in valuesList)
                {
                    if (values.Count > 0)
                    {
                        found = true;
                        AFAttributeWrapper attribute = values[0].Attribute;
                        if (attribute.IsTDengineTag())
                        {
                            var valuestring = attribute.ToStringWithUOM();
                            log.Debug($"element tag {element.Name}: {attribute.Name}:{valuestring}");
                            continue;
                        }
                        string superTableName;
                        if (!attribute.Element.hasTemplate()) {
                            superTableName = TableNameConvert.GetSingleElementSuperTableName(element);
                        } else {
                            superTableName = TableNameConvert.GetAFPointSuperTableName(attribute.Element.Template);
                        }
                        ConvertAFAttibutesAndValuesToTDTables(attribute, values, out Dictionary<string, Dictionary<string, List<TDValue>>> tables, out List<string> columnNames);
                        var stables = new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
                        stables.Add(superTableName, tables);
                        this.tdEngineProxy.InsertValuesForAFElements(tdDatabaseName, stables, columnNames).Wait();
                        log.Debug($"Backfill TDEngine attribute {element.Name}\\{attribute.Name}, {values.Count} values written in {stopwatch.ElapsedMilliseconds} ms");
                 
                        if (values[values.Count - 1].Timestamp.LocalTime < smallLastAttributeTime
                            && values[values.Count - 1].AFSDKObject.IsGood == true)
                        {
                            smallLastAttributeTime = values[values.Count - 1].Timestamp.LocalTime;
                        }
                        count += values.Count;
                    }
                }
                log.Info($"Backfill TDEngine {element.Name}:{element.ID} from {currentStart} count:{count} , written in {stopwatch.ElapsedMilliseconds} ms");

                if (count < 100) {
                    break;
                }
                // Attribute last time could not be equal, select the smaller one. Allowed to repeat, not allowed to omit.
                currentStart = smallLastAttributeTime < endTime ? smallLastAttributeTime.AddMilliseconds(1) : endTime;
                currentStart = smallLastAttributeTime < endTime ? smallLastAttributeTime.AddMilliseconds(1) : endTime;
                stopwatch.Reset();
                if (!found) break;
            } while (currentStart < endTime);
            log.Info($"Backfill TDEngine element {element.Name}:{element.ID} values written finished.");
        }

        private void ConvertAFAttibutesAndValuesToTDTables(AFAttributeWrapper attribute, AFValuesWrapper values, out Dictionary<string, Dictionary<string, List<TDValue>>> tables, out List<string> columnNames)
        {
            tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();
            columnNames = new List<string>();

            var elementTableKey = attribute.Element.ID.ToString();
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


                if (tables.ContainsKey(elementTableKey))
                {
                    var table = tables[elementTableKey];
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
                    tables.Add(elementTableKey, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                }
            }
        }
    }
}

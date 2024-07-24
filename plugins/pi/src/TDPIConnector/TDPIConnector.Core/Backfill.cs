using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Conversions;
using log4net;
using System.Threading;
using System.IO;

namespace TDPIConnector.Core
{
    public class Backfill
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private TDEngineProxy tdEngineProxy;
        private PIServerManager piServerManager;
        private PISystemManager piSystemManager;
        private Dictionary<String, DateTime> breakpoints;
        ConcurrentDictionary<string, ElementsBackfillTaskManager> templateElementsBackfill = new ConcurrentDictionary<string, ElementsBackfillTaskManager>();
        ConcurrentDictionary<int, string> templateBackfillGroups = new ConcurrentDictionary<int, string>();
        private readonly Object groupLock = new Object();
        int backfillWait = 0;  // ms
        List<Task> backFillTasks = new List<Task>();
        bool stopAddNewTask = false;

        private string tdDatabaseName;
        private int nextGroupStart;

        private class ElementBackfillTask
        {
            public Guid elementID;
            public DateTime startTime;
            public DateTime endTime;

            public ElementBackfillTask(in Guid elementID, DateTime startTime, DateTime endTime)
            {
                this.elementID = elementID;
                this.startTime = startTime;
                this.endTime = endTime;
            }
        }
        private class BackfillTask {
            public List<Guid> elementIDS;
            public DateTime startTime;
            public DateTime endTime;

            public BackfillTask(in List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
            {
                this.elementIDS = new List<Guid>();
                foreach (var element in elements) {
                    elementIDS.Add(element.ID);
                };
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
            public string templateName;
            private readonly Object taskLock = new Object();

            public bool Start { get; internal set; } = false;

            public ElementsBackfillTaskManager(string templateName)
            {
                this.templateName = templateName;
            }

            public void AddNewElementsTask(in List<AFElementWrapper> elements, DateTime startTime, DateTime endTime) {
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
                    if (nextIndexInBatch == elementsTasks[currentBatchIndex].elementIDS.Count()) {
                        nextBatchIndex += 1;
                        nextIndexInBatch = 0;
                    }
                    ++started;
                    log.Info($"Backfill element " +
                        $"{elementsTasks[currentBatchIndex].elementIDS[currentIndexInBatch].ToString()} startting: {started}/{all}");
                    ElementBackfillTask task = new ElementBackfillTask(elementsTasks[currentBatchIndex].elementIDS[currentIndexInBatch],
                        elementsTasks[currentBatchIndex].startTime, elementsTasks[currentBatchIndex].endTime);
                    currentBatchIndex = nextBatchIndex;
                    currentIndexInBatch = nextIndexInBatch;
                    return task;
                }
            }

            public void FinishedOne(in AFElementWrapper element, int groupNum) {
                lock (taskLock)
                {
                    ++finished;
                    log.Info($"Backfill element {element.TemplateName()}:" +
                        $"{element.ID.ToString()} group({groupNum}) finshed: {finished}/{all}");
                }
            }

            internal bool Finished()
            {
                return all == finished;
            }
        }
        public Backfill(in TDEngineProxy tdEngineProxy, in PIServerManager piServerManager, in PISystemManager piSystemManager)
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
            if (AppSettings.tomlConfig.ForBackfill) {
                InitBreakpoint();
            }
            StartAsyncBackTask();
            Console.WriteLine("Asynchronous task starting...");
        }

        private void InitBreakpoint()
        {
            var backfillBreakpointFile = AppSettings.tomlConfig.BackfillBreakpointFile;
            if (backfillBreakpointFile == null || backfillBreakpointFile == "")
            {
                log.Warn("No backfill breakpoint file found.");
                return;
            }
            breakpoints = new Dictionary<string, DateTime>();
            log.Info($"Read backfill breakpoint file: {backfillBreakpointFile}");
            try
            {
                using (var reader = new StreamReader(backfillBreakpointFile))
                {
                    while (!reader.EndOfStream)
                    {
                   
                        var line = reader.ReadLine();
                        if (line == null || line.Length == 0) {
                            continue;
                        }
                        var sp = line.Split(',');
                        if (sp.Length == 2) {
                            breakpoints.Add(sp[0], DateTime.Parse(sp[1]));                            
                        }
                        else
                        {
                            log.Warn($"Invalid line in backfill breakpoint file: {line}");
                        }


                    }
                }
            }
            catch (IOException e)
            {
               log.Error($"Read breankpoint file error: {e.Message}");
            }
            log.Info($"Read {breakpoints.Count} breakpoints from file.");
        }

        private ElementBackfillTask GetNextTask(int groupNum)
        {
            ElementsBackfillTaskManager groupManager = null;
            lock (groupLock) {
                if (templateBackfillGroups.ContainsKey(groupNum)) {
                    groupManager = GetElementBackfillManager(templateBackfillGroups[groupNum]);
                }
            }

            if (null != groupManager) {
                var task = groupManager.GetNextTask();
                if (task != null) return task;
                tdEngineProxy.LimitTaosxClientCapToOne(groupManager.templateName);
                log.Info($"[BackfillManager]Templalte finished: {groupManager.templateName}. Group({groupNum}) waitting new task.");
            }
            
            ElementsBackfillTaskManager newGroupManager = GetNotStartedGroup(groupNum);
            if (null != newGroupManager) {
                log.Info($"[BackfillManager]Template start:{newGroupManager.templateName}. Group({groupNum}).");
                tdEngineProxy.ExpandTaosxClientCap(newGroupManager.templateName, AppSettings.tomlConfig.ConcurrencyCountsForOneTemplate);
                return newGroupManager.GetNextTask();
            }

            if (stopAddNewTask)
            {
                while (true) {
                    Task.Delay(500);
                    ElementsBackfillTaskManager newManagerToadd = GetNotFinishedGroup(groupNum);
                    if (null == newManagerToadd)
                    {
                        return null;
                    }
                    else
                    {
                        var task = newManagerToadd.GetNextTask();
                        if (task != null)
                        {
                            tdEngineProxy.ExpandTaosxClientCap(newManagerToadd.templateName, AppSettings.tomlConfig.ConcurrencyCountsForOneTemplate);
                            log.Info($"[BackfillManager]Add new backfill group for template:{newManagerToadd.templateName}, group({groupNum}).");
                            return task;
                        }
                        else {
                            Task.Delay(500);
                        }
                    }
                }

            }
            else {
                return null;
            }
        }

        private ElementsBackfillTaskManager GetNotFinishedGroup(int groupNum)
        {
            lock (groupLock) {
                var templates = templateElementsBackfill.ToList();
                if (templates.Count == 0) return null;
                int i = 0;
                while (true) {
                    ++i;
                    if (!templates[nextGroupStart].Value.Finished())
                    {
                        int index = nextGroupStart;
                        templates[index].Value.Start = true;
                        templateBackfillGroups[groupNum] = templates[index].Value.templateName;

                        nextGroupStart = (index >= templates.Count() - 1) ? 0 : index + 1;
                        return templates[index].Value;
                    }
                    else {
                        nextGroupStart = (nextGroupStart >= templates.Count() - 1) ? 0 : nextGroupStart + 1;
                    }
                    if (i == templateElementsBackfill.Count()) break;
                }
                return null;
            }
        }

        private ElementsBackfillTaskManager GetNotStartedGroup(int groupNum)
        {
            lock (groupLock)
            {
                foreach (var templateTask in templateElementsBackfill)
                {
                    if (!templateTask.Value.Start)
                    {
                        templateTask.Value.Start = true;
                        templateBackfillGroups[groupNum] = templateTask.Value.templateName;
                        return templateTask.Value;
                    }
                }
                return null;
            }
        }

        private void FinishedOne(in AFElementWrapper elemment, int groupNum)
        {
            var elemmentBackfillManager = GetElementBackfillManager(elemment.TemplateName());
            elemmentBackfillManager.FinishedOne(elemment, groupNum);
        }

        public void StartAsyncBackTask()
        {
            for (int i = 0; i < AppSettings.tomlConfig.BackfillConcurrencyCounts; ++i)
            {
                int groupNum = i;
                backFillTasks.Add(Task.Run(async () =>
                {
                    while (true)
                    {
                        try
                        {
                            var task = GetNextTask(groupNum);
                            if (task != null)
                            {
                                AFElementWrapper element = piSystemManager.GetElementsById(task.elementID);
                                if (element != null)
                                {
                                    try
                                    {
                                        BackfillElement(tdDatabaseName, element, task.startTime, task.endTime);
                                        FinishedOne(element, groupNum);
                                    }
                                    catch (Exception e)
                                    {
                                        log.Error($"[BackfillManager]Backfill element error:{element}", e);
                                    }

                                }
                            }
                            else
                            {
                                if (stopAddNewTask)
                                {
                                    log.Info($"[BackfillManager]Finished, group({groupNum}) quit!");
                                    return;
                                }
                                await Task.Delay(500);
                            }
                        }
                        catch (Exception e) {
                            log.Error($"[BackfillManager]Exception in backfill task", e);
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
                log.Info($"BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
            }
            log.Info($"BackfillPIPointsFromLastRecordedValue finished.");
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
                log.Info($"BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
            }
            log.Info($"BackfillPIPoints finished.");
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
            log.Info("[BackfillManager]Init Finished, stop add new element into backfill list.");
            Task.WaitAll(backFillTasks.ToArray());
            log.Info("[BackfillManager]All task Finished.");
            tdEngineProxy.StopAll();
            log.Info("[BackfillManager]Close connection with agent.");
        }
        internal void StopAddTask()
        {
            stopAddNewTask = true;
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

        internal void BackfillElements(string tdDatabaseName, in List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
        {
            var elemmentBackfillManager = GetElementBackfillManager("default");
            elemmentBackfillManager.AddNewElementsTask(elements, startTime, endTime); 
        }

        private ElementsBackfillTaskManager GetElementBackfillManager(in string templateName) {
            if (!templateElementsBackfill.ContainsKey(templateName))
            {
                templateElementsBackfill[templateName] = new ElementsBackfillTaskManager(templateName);  
            }
            return templateElementsBackfill[templateName];
        }

        internal void BackfillElementsOfTemplate(in string templateName, in List<AFElementWrapper> elements, DateTime startTime, DateTime endTime)
        {
            ElementsBackfillTaskManager elemmentBackfillManager = GetElementBackfillManager(templateName);
            elemmentBackfillManager.AddNewElementsTask(elements, startTime, endTime);
        }

        internal void BackfillElement(string tdDatabaseName, in AFElementWrapper element, DateTime startTime, DateTime endTime)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            AFAttributeListWrapper attributes = new AFAttributeListWrapper();
            foreach (AFAttributeWrapper attribute in element.Attributes)
            {
                if (!attribute.IsTDengineTag() && !attribute.Unsupported()) // tag update is not here
                {
                    attributes.Add(attribute);
                }
                if (attribute.HasChild())
                {
                    foreach (AFAttributeWrapper childAttribute in attribute.childAttributes)
                    {
                        if (!childAttribute.IsTDengineTag() && !childAttribute.Unsupported()) // tag update is not here
                        {
                            attributes.Add(childAttribute);
                        }
                    }
                }
            }
            string superTableName;
            if (!element.hasTemplate())
            {
                superTableName = TableNameConvert.GetSingleElementSuperTableName(element);
            }
            else
            {
                superTableName = TableNameConvert.GetAFPointSuperTableName(element.Template);
            }

            var currentStart = startTime;
            var elementID = element.ID.ToString();
            if (breakpoints != null && breakpoints.ContainsKey(elementID))
            {
                var breakPoint = breakpoints[elementID];
                if (breakPoint >= currentStart)
                {
                    currentStart = breakPoint.AddMilliseconds(1);
                    log.Info($"Backfill element {superTableName}:{element.ID} from breakpoint {currentStart}.");
                }
            }
            if (currentStart >= endTime) return;
            do
            {
                tdEngineProxy.ArrowMsgQueueWait(element.TemplateName());
                stopwatch.Reset();
                stopwatch.Start();
                // 一个 attributes 对应一个 values
                IEnumerable<AFValuesWrapper> valuesList = piSystemManager.GetAttributesRecordedValues(attributes, currentStart, endTime, AppSettings.tomlConfig.BackfillBatchSize);
                bool found = false;
                // 所有属性的最后时间中最小的时间
                DateTime smallLastAttributeTime = endTime;
                int count = 0;

                var columnNames = new List<string>();
                var elementValues = new Dictionary<string, List<TDValue>>();
                foreach (AFValuesWrapper values in valuesList)
                {
                    if (values.Count > 0)
                    {
                        found = true;
                        AFAttributeWrapper attribute = values[0].Attribute;
                        ConvertAFAttibutesAndValuesToTDTables(attribute, values, in elementValues, in columnNames);
                        if (values[values.Count - 1].Timestamp.LocalTime < smallLastAttributeTime
                            && values[values.Count - 1].AFSDKObject.IsGood == true)
                        {
                            smallLastAttributeTime = values[values.Count - 1].Timestamp.LocalTime;
                        }
                        count += values.Count;
                    }
                }
                if (!found) break;
                // table -> column -> values
                var tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();
                // stable -> table -> column -> values
                var stables = new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
                var elementTableKey = element.ID.ToString();
                tables.Add(elementTableKey, elementValues);
                stables.Add(superTableName, tables);
                tdEngineProxy.InsertValuesForAFElements(stables, columnNames).Wait();
                log.Info($"Backfill element {superTableName}:{elementID} from {currentStart} rows {elementValues.Count} in {stopwatch.ElapsedMilliseconds} ms");

                if (count < AppSettings.tomlConfig.BackfillBatchSize) {
                    break;
                }
                // Attribute last time could not be equal, select the smaller one. Allowed to repeat, not allowed to omit.
                currentStart = smallLastAttributeTime < endTime ? smallLastAttributeTime.AddMilliseconds(1) : endTime;
                stopwatch.Reset();
            } while (currentStart < endTime);
        }

        private void ConvertAFAttibutesAndValuesToTDTables(in AFAttributeWrapper attribute, in AFValuesWrapper values,  in Dictionary<string, List<TDValue>> tableValues, in List<string> columnNames)
        {
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

                if (attribute.IsChild()) {
                    tdValue.Name = AttributeColumnConverter.GetChildAttrbuteName(attribute.Parent, attribute);
                } else {
                    tdValue.Name = attribute.Name;
                }

                if (tableValues.ContainsKey(timestamp))
                {
                    tableValues[timestamp].Add(tdValue);
                }
                else
                {
                    tableValues.Add(timestamp, new List<TDValue>() { tdValue });
                }
            }
        }
    }
}

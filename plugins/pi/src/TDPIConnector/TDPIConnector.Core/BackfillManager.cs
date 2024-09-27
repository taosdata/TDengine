using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;
using System.Collections.Concurrent;

namespace TDPIConnector.Core
{
    public class BackfillManager
    {
        private readonly PISystemManager piSystemManager;
        private readonly PIServerManager piServerManager;
        private readonly TDEngineProxy tdEngineProxy;
        private readonly TablesCreator tablesCreator;
        private readonly Backfill backfill;

        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public BackfillManager(PISystemManager piSystemManager, PIServerManager piServerManager, TDEngineProxy tdEngineProxy, TablesCreator tablesCreator)
        {
            this.piSystemManager = piSystemManager;
            this.piServerManager = piServerManager;
            this.tdEngineProxy = tdEngineProxy;
            this.tablesCreator = tablesCreator;
            this.backfill = new Backfill(tdEngineProxy, piServerManager, piSystemManager);
        }

        public BackfillManager(PISystemManager piSystemManager, PIServerManager piServerManager, TDEngineProxy tdEngineProxy)
        {
            this.piSystemManager = piSystemManager;
            this.piServerManager = piServerManager;
            this.tdEngineProxy = tdEngineProxy;
            this.tablesCreator = new TablesCreator(piSystemManager, piServerManager, tdEngineProxy);
            this.backfill = new Backfill(tdEngineProxy, piServerManager, piSystemManager);
        }

        public Task BackfillPIPointsFromService(string tdDatabaseName, List<TDTable> piPointTables, DateTime backfillStartTime, DateTime backfillEndTime)
        {
            return Task.Run(() => {
                log.Info("Process backfill, PI Point Mode backfill start...");
                try
                {
                    // TODO: 从断点开始 backfill
                    //var pointsToBackfillChecked = new Dictionary<PIPointWrapper, DateTime>();
                    //List<string> pointNames = piPointTables.Select(p => p.Name).ToList();
                    //List<PIPointWrapper> piPointList = piServerManager.FindPIPoints(pointNames);
                    //foreach (var point in piPointList)
                    //{
                    //    pointsToBackfillChecked.Add(point, backfillStartTime);
                    //}
                    //if (pointsToBackfillChecked.Count > 0)
                    //{
                    //    backfill.BackfillPIPointsFromLastRecordedValue(tdDatabaseName, pointsToBackfillChecked, backfillEndTime);
                    //}
                    backfill.BackfillPIPoints(tdDatabaseName, backfillStartTime, backfillEndTime, piPointTables);
                }
                catch (Exception e)
                {
                    log.Error($"Error backfilling PI Points...{e.Message}");
                }
                log.Info("Process backfill, PI Point Mode backfill finshed");
                return Task.CompletedTask;
            });
        }

        public async Task BackfillPIPointsFromTool(string tdDatabaseName, string afDatabaseName, DateTime startTime, DateTime endTime, bool toFirstRecorded, bool fromLastRecorded, bool dropTables)
        {
            //ignore drop table flag it backfill to or from recorded values
            if (fromLastRecorded || toFirstRecorded)
            {
                dropTables = false;
            }

            //create tables if needed
            var piPoints = await tablesCreator.GetPIPointTables(tdDatabaseName);

            if (piPoints == null || piPoints.Count == 0)
            {
                return;
            }

            int all = piPoints.Count;
            int finished = 0;
            if (fromLastRecorded)
            {
                Dictionary<string, DateTime> pointsTimestamps = await backfill.GetTDTableLastRecordedValueFromPIPoints(tdDatabaseName, piPoints);
                List<PIPointWrapper> piPointList = piServerManager.FindPIPoints(pointsTimestamps.Keys.ToList());
                Dictionary<PIPointWrapper, DateTime> piPointsTimestamps = new Dictionary<PIPointWrapper, DateTime>();
                foreach (var pointsTimestamp in pointsTimestamps)
                {
                    PIPointWrapper piPoint = piPointList.Where(p => p.Name.ToLower() == pointsTimestamp.Key.ToLower()).Single();
                    var pointStartTime = startTime > pointsTimestamp.Value ? startTime : pointsTimestamp.Value.AddMilliseconds(1);
                    backfill.BackfillPIPoint(tdDatabaseName, pointStartTime, endTime, piPoint);
                    finished++;
                    log.Info($"Backfill BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
                }
            }
            else if (toFirstRecorded)
            {
                Dictionary<string, DateTime> pointsTimestamps = await backfill.GetTDPointsFirstRecordedValueFromPIPoints(tdDatabaseName, piPoints);
                List<PIPointWrapper> piPointList = piServerManager.FindPIPoints(pointsTimestamps.Keys.ToList());
                foreach (var pointsTimestamp in pointsTimestamps)
                {
                    PIPointWrapper piPoint = piPointList.Where(p => p.Name.ToLower() == pointsTimestamp.Key.ToLower()).Single();
                    var pointEndTime = endTime < pointsTimestamp.Value ? endTime : pointsTimestamp.Value.AddMilliseconds(-1);
                    backfill.BackfillPIPoint(tdDatabaseName, startTime, pointEndTime, piPoint);
                    finished++;
                    log.Info($"Backfill BackfillPIPointsFromLastRecordedValue finished {finished}/{all}.");
                }
            }
            else
            {
                backfill.BackfillPIPoints(tdDatabaseName, startTime, endTime, piPoints);
            }
            log.Info("BackfillPIPointsFromTool completed");
        }

        // 从专门的 backfill 工具启动 backfill 任务
        public async Task BackfillAFElementsFromTool(string tdDatabaseName, string afDatabaseName, List<string> elementTemplateNames, DateTime startTime, DateTime endTime, bool toFirstRecorded, bool fromLastRecorded, bool dropTables)
        {
            //get all AF Templates based on settings
            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName, elementTemplateNames);

            //get all AF Elements based on templates
            Dictionary<string, AFElementWrapper> elementLookup = new Dictionary<string, AFElementWrapper>();
            foreach (AFElementTemplateWrapper elementTemplate in elementTemplates)
            {
                //check for associated supertable, create if needed
                var superTable = TemplateSTableConverter.Convert(elementTemplate);
                if (!superTable.HasValidColumn()) continue;
                var resp2 = await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

                var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);

                //get all elements based on template
                IEnumerable<AFElementWrapper> elements = piSystemManager.GetElementTemplateInstances(elementTemplate);
                if (AppSettings.tomlConfig.ForBackfill)
                {
                    List<TDTable> tables = new List<TDTable>();
                    foreach (AFElementWrapper element in elements)
                    {
                        //check for associated table, create if needed
                        var table = ElemenetTableConverter.Convert(element, superTable.Name, ref templateAttributeColumns);
                        if (elementLookup.ContainsKey(table.ID))
                        {
                            log.Info($"BackfillAFElement, found duplicate element:{table.Name}");
                            continue;
                        }
                        tables.Add(table);
                        if (dropTables)
                        {
                            await tdEngineProxy.DropTableForAFElement(tdDatabaseName, table);
                        }

                        elementLookup.Add(table.ID, element);
                    }
                    await tdEngineProxy.CreateTablesForAFElements(tdDatabaseName, tables);
                }
                else {
                    foreach (AFElementWrapper element in elements)
                    {
                        elementLookup.Add(element.ID.ToString(), element);
                    }    
                }
            }

            //backfill elements
            if (fromLastRecorded || toFirstRecorded)
            {
                Dictionary<string, DateTime> elementsTimestamps = new Dictionary<string, DateTime>();
                var tableNameList = elementLookup.Keys.ToList();
                if (fromLastRecorded)
                    elementsTimestamps = await backfill.GetTDTableLastRecordedValueFromAFElements(tdDatabaseName, tableNameList, elementTemplateNames);
                else if (toFirstRecorded)
                    elementsTimestamps = await backfill.GetTDPointsFirstRecordedValue(tdDatabaseName, tableNameList);

                //backfill points if needed
                if (elementsTimestamps.Count > 0)
                {
                    foreach (var elementTimestamp in elementsTimestamps)
                    {
                        var element = elementLookup[elementTimestamp.Key];
                        if (fromLastRecorded)
                            backfill.BackfillElement(tdDatabaseName, element,
                                elementTimestamp.Value >= startTime ? elementTimestamp.Value.AddMilliseconds(1) : startTime,
                                endTime);

                        else if (toFirstRecorded)
                            backfill.BackfillElement(tdDatabaseName, element,
                                startTime,
                                elementTimestamp.Value <= endTime ? elementTimestamp.Value.AddMilliseconds(-1) : endTime);
                    }
                }
            }
            else
            {
                backfill.BackfillElements(tdDatabaseName, elementLookup.Values.ToList(), startTime, endTime);
            }
        }
        public async Task BackfillAFElementFromTool(string tdDatabaseName, string afDatabaseName, List<string> elementNames, DateTime startTime, DateTime endTime, bool toFirstRecorded, bool fromLastRecorded, bool dropTables)
        {
            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName, elementNames);

            //get all AF Elements based on templates
            Dictionary<string, AFElementWrapper> elementLookup = new Dictionary<string, AFElementWrapper>();
            foreach (string elementName in elementNames)
            {
                var wrappers = piSystemManager.GetElementByName(afDatabaseName, elementName);
                List<TDTable> tables = new List<TDTable>();
                foreach (AFElementWrapper element in wrappers)
                {
                    if (element.hasTemplate()) continue;
                    var superTable = ElemenetSTableConverter.Convert(element);
                    if (!superTable.HasValidColumn()) continue;
                    var resp2 = await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

                    var attributeColumns = AttributeColumnConverter.Convert(element.Attributes);

                    var table = ElemenetTableConverter.Convert(element, superTable.Name, ref attributeColumns);
                    if (elementLookup.ContainsKey(table.Name))
                    {
                        log.Info($"BackfillAFElement, found duplicate element:{table.Name}");
                        continue;
                    }
                    tables.Add(table);
                    if (dropTables)
                    {
                        await tdEngineProxy.DropTableForAFElement(tdDatabaseName, table);
                    }

                    elementLookup.Add(table.Name, element);
                    await tdEngineProxy.CreateTablesForAFElements(tdDatabaseName, tables);
                }              
            }

            //backfill elements
            if (fromLastRecorded || toFirstRecorded)
            {
                Dictionary<string, DateTime> elementsTimestamps = new Dictionary<string, DateTime>();
                var tableNameList = elementLookup.Keys.ToList();
                if (fromLastRecorded)
                    elementsTimestamps = await backfill.GetTDPointsLastRecordedValue(tdDatabaseName, tableNameList);
                else if (toFirstRecorded)
                    elementsTimestamps = await backfill.GetTDPointsFirstRecordedValue(tdDatabaseName, tableNameList);

                //backfill points if needed
                if (elementsTimestamps.Count > 0)
                {
                    foreach (var elementTimestamp in elementsTimestamps)
                    {
                        var element = elementLookup[elementTimestamp.Key];
                        if (fromLastRecorded)
                            backfill.BackfillElement(tdDatabaseName, element,
                                elementTimestamp.Value >= startTime ? elementTimestamp.Value.AddMilliseconds(1) : startTime,
                                endTime);

                        else if (toFirstRecorded)
                            backfill.BackfillElement(tdDatabaseName, element,
                                startTime,
                                elementTimestamp.Value <= endTime ? elementTimestamp.Value.AddMilliseconds(-1) : endTime);
                    }
                }
            }
            else
            {
                backfill.BackfillElements(tdDatabaseName, elementLookup.Values.ToList(), startTime, endTime);
            }
        }
        public Backfill GetBackfill()
        {
            return backfill;
        }
    }
}

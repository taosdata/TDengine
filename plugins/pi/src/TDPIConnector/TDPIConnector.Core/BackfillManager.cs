using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;

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

        public Task BackfillPIPointsFromService(string tdDatabaseName, List<TDTable> piPointTables, DateTime backfillStartLimit)
        {
            return Task.Run(async () =>
            {
                log.Info("Process backfill, PI Point Mode backfill start...");
                try
                {
                    //backfill points if needed
                    Dictionary<string, DateTime> pointsTobackfill = await backfill.GetTDTableLastRecordedValueFromPIPoints(tdDatabaseName, piPointTables);
                    var pointsToBackfillChecked = new Dictionary<PIPointWrapper, DateTime>();

                    DateTime endTime = DateTime.Now;

                    //replace pointsTobackfill with bacfillStartLimit if needed
                    List<PIPointWrapper> piPointList = piServerManager.FindPIPoints(pointsTobackfill.Keys.ToList());
                    foreach (var point in pointsTobackfill)
                    {
                        PIPointWrapper piPoint = piPointList.Where(p => p.Name.ToLower() == point.Key.ToLower()).Single();
                        pointsToBackfillChecked.Add(piPoint, point.Value < backfillStartLimit ? backfillStartLimit : point.Value);
                    }


                    if (pointsToBackfillChecked.Count > 0)
                    {
                        backfill.BackfillPIPointsFromLastRecordedValue(tdDatabaseName, pointsToBackfillChecked, endTime);
                    }
                }
                catch (Exception e)
                {
                    log.Error($"Error backfilling PI Points...{e.Message}");
                }
                log.Info("Process backfill, PI Point Mode backfill finshed");
            });
        }

        public Task BackfillAFElementsFromService(string tdDatabaseName, Dictionary<string, AFElementWrapper> elements, DateTime backfillStartLimit)
        {
            return Task.Run(async () =>
            {
                log.Info("Process backfill, AF Element Mode backfill start...");
                try
                {
                    IEnumerable<string> elementTemplateNames = elements.ToList().Select(e => e.Value.Template.Name).Distinct();
                    var elementsLastRecordedTimestamps = await backfill.GetTDTableLastRecordedValueFromAFElements(tdDatabaseName, elements.Keys.ToList(), elementTemplateNames);
                    var elementsLastRecordedTimestampsChecked = new Dictionary<string, DateTime>();

                    //replace element value (last recorded value) with bacfillStartLimit if needed
                    foreach (var element in elementsLastRecordedTimestamps)
                    {
                        elementsLastRecordedTimestampsChecked.Add(element.Key, element.Value < backfillStartLimit ? backfillStartLimit : element.Value);
                    }

                    Dictionary<AFElementWrapper, DateTime> elementsToBackfill = new Dictionary<AFElementWrapper, DateTime>();
                    foreach (var item in elementsLastRecordedTimestampsChecked)
                    {
                        elementsToBackfill.Add(elements[item.Key], item.Value);
                    }
                    if (elementsToBackfill.Count > 0)
                    {
                        //backfill points if needed
                        backfill.BackfillAFElementsFromLastRecordedValue(tdDatabaseName, elementsToBackfill);
                    }
                }
                catch (Exception e)
                {
                    log.Error($"Error backfilling AF Elements...{e.Message}");
                }
                log.Info("Process backfill, AF Element Mode backfill finished");
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
            var piPoints = await tablesCreator.CreatePIPointTables(tdDatabaseName, afDatabaseName, dropTables);

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
                var resp2 = await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

                var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);

                //get all elements based on template
                IEnumerable<AFElementWrapper> elements = piSystemManager.GetElementTemplateInstances(elementTemplate);
                List<TDTable> tables = new List<TDTable>();
                foreach (AFElementWrapper element in elements)
                {
                    //check for associated table, create if needed
                    var table = ElemenetTableConverter.Convert(element, superTable.Name, templateAttributeColumns);
                    tables.Add(table);
                    if (dropTables)
                    {
                        await tdEngineProxy.DropTableForAFElement(tdDatabaseName, table);
                    }
                 
                    elementLookup.Add(table.Name, element);
                }
                await tdEngineProxy.CreateTablesForAFElements(tdDatabaseName, tables);
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
    }
}

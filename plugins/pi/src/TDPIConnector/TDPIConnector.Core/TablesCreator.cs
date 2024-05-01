using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;
using System.Diagnostics;

namespace TDPIConnector.Core
{
    public class TablesCreator
    {
        private readonly PISystemManager piSystemManager;
        private readonly PIServerManager piServerManager;
        private readonly TDEngineProxy tdEngineProxy;
        HashSet<int> pointSet;
        Dictionary<int, string> pointElementPath = new Dictionary<int, string>();

        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public TablesCreator(PISystemManager piSystemManager, PIServerManager piServerManager, TDEngineProxy tdEngineProxy)
        {
            this.piSystemManager = piSystemManager;
            this.piServerManager = piServerManager;
            this.tdEngineProxy = tdEngineProxy;
        }

        public async Task CreateDatabase(string databaseName)
        {
            await tdEngineProxy.CreateDatabase(databaseName);
        }
        public List<TDColumn> GetPiPointTags(PIPointWrapper point) {
            var pointTags = new List<TDColumn>();
            var tagVals = point.GetPointSavedAttrsValue();
            var tagTypes = PIPointWrapper.GetPointSavedAttrsType();
            foreach (var tag in tagVals) {
                TDColumn column = new TDColumn(tag.Key, tagTypes[tag.Key], "", null);
                column.TagValue = tag.Value;
                pointTags.Add(column);
            }
            return pointTags;
        }

        public async Task<List<TDTable>> GetPIPointTables(string tdDatabaseName)
        {
            var points = new List<PIPointWrapper>();
            if (AppSettings.tomlConfig.PointList != null && piServerManager != null)
            {
                points = piServerManager.FindPIPoints(AppSettings.tomlConfig.PointList);
            }
            var piPoints = await CreatePIPointTablesByPoints(tdDatabaseName, points);
            return piPoints;
        }
        public async Task<List<TDTable>> CreatePIPointTables(string tdDatabaseName, string afDatabaseName, bool dropTableFirst = false)
        {
            var points = new List<PIPointWrapper>();
            //get AF Mode 1 Points
            if (AppSettings.tomlConfig.TemplateForPIPoint != null && piSystemManager != null)
            {
                points = piSystemManager.GetPIPointsFromElementTemplates(afDatabaseName, AppSettings.tomlConfig.TemplateForPIPoint);
                log.Info($"Get Pi Points from element template, count:{points.Count}");
            }
            else
            {
                log.Info("No ElementTemplates1.csv file found or AF Server not defined.");
            }

            //add to list of piPoints for point mode
            if (AppSettings.tomlConfig.PointList != null)
            {
                points.AddRange(piServerManager.FindPIPoints(AppSettings.tomlConfig.PointList));
            }
            else
            {
                log.Info("No points.csv file found.");
            }
            var piPoints = await CreatePIPointTablesByPoints(tdDatabaseName, points, dropTableFirst);
            return piPoints;
        }
        public void GetElementsPathForPoint() {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var allElements = piSystemManager.GetAllElements(AppSettings.tomlConfig.AFDatabaseName);
            int elementCount = allElements.Count();
            log.Info($"GetElementsPathForPoint start, checking {elementCount} elements for {pointSet.Count} points.");
            int checking = 0;
            foreach (var e in allElements) {
                checking++;
                string elementFullPath = $"{e.GetPath()}";
                string[] parts = elementFullPath.Split('\\');
                string elementPath = string.Join("\\", parts.Skip(4));
                foreach (var attr in e.Attributes) {
                    if (attr.PIPoint != null) {
                        int pointID = attr.PIPoint.PointId;
                        if (!pointSet.Contains(pointID)) continue;
                        if (pointElementPath.ContainsKey(pointID))
                        {
                            pointElementPath[pointID] += $"|{elementPath}";
                        }
                        else {
                            pointElementPath.Add(pointID, elementPath);
                        }
                    }
                }
                if (checking % 1000 == 0)
                {
                    log.Info($"GetElementsPathForPoint, checking checking {checking}th elements.");
                }
            }
            stopwatch.Stop();
            TimeSpan elapsed = stopwatch.Elapsed;
            log.Info($"GetElementsPathForPoint, checkout {elementCount} elements for {pointSet.Count} points, cost time:{elapsed.TotalSeconds} seconds.");
        }
        public async Task<List<TDTable>> CreatePIPointTablesByPoints(string tdDatabaseName, List<PIPointWrapper> points, bool dropTableFirst = false)
        {
            var piPoints = new List<TDTable>();

            if (points.Count == 0)
            {
                log.Info("No points found.");
                return piPoints;
            }
            else
            {
                log.Info($"Found {points.Count()} PI Points.");
            }
            if (piSystemManager != null) {
                pointSet = new HashSet<int>(points.Select(p => p.PointId).Distinct());
                GetElementsPathForPoint();
            }

            foreach (var point in points)
            {
                string tdColumnType = PointTypeConverter.Convert(point.PointType);
                string superTableName = PIInfoScanner.GeneratePointSuperTableName(point);
                var tagVals = GetPiPointTags(point);
                var table = new TDTable(point.Name, point.ID, superTableName, tdColumnType, tagVals);
                table.Location = point.GetPath();
                table.ElementPath = pointElementPath.ContainsKey(point.PointId) ? pointElementPath[point.PointId] : "";
                piPoints.Add(table);
                log.Info($"Add new table {table.STableName} {table.Name}");
            }
            //drop tables first if requried
            if (dropTableFirst)
            {
                log.Info($"Dropping {points.Count()} PI Points tables.");
                foreach (var piPoint in piPoints.ToList())
                {
                    await tdEngineProxy.DropTableForPIPoint(tdDatabaseName, piPoint.Name);
                }
            }

            var tags = PIPointWrapper.GetPointSavedAttrsType();
            List<string> STableNames = piPoints.Select(p => p.STableName).Distinct().ToList();
            foreach (string STableName in STableNames)
            {
                var piPoint = piPoints.Where(p => p.STableName == STableName).First();
                await tdEngineProxy.CreateSuperTableForPIPoint(tdDatabaseName, piPoint.STableName, piPoint.ColumnType, tags.ToList(), piSystemManager != null);
            }

            await tdEngineProxy.CreateTablesForPIPoints(tdDatabaseName, piPoints);


            //check TD Engine for backfilling of points
            return piPoints;
        }
        internal async Task<Dictionary<string, AFElementWrapper>> GetElementsInfoByIds(string tdDatabaseName, string afDatabaseName, List<String> ids)
        {
            IEnumerable<AFElementWrapper> elements = null;
            List<List<string>> chunks = new List<List<string>>();
            int chunkSize = 2000;
            for (int i = 0; i < ids.Count; i += chunkSize)
            {
                List<string> chunk = ids.Skip(i).Take(chunkSize).ToList();
                chunks.Add(chunk);
            }
            foreach (var chunk in chunks) {
                if (elements == null) {
                    elements = piSystemManager.GetElementsByIds(afDatabaseName, chunk);
                } else {
                    elements = elements.Concat(piSystemManager.GetElementsByIds(afDatabaseName, chunk));
                }
            }
            log.Info($"Found {elements.Count()} elements.");

            Dictionary<string, AFElementWrapper> elementsCollection = new Dictionary<string, AFElementWrapper>();
            Dictionary<string, List<AFElementWrapper>> existTemplates = new Dictionary<string, List<AFElementWrapper>>();
            HashSet<Guid> usedElements = new HashSet<Guid>();
            foreach (var element in elements)
            {
                if (!usedElements.Contains(element.ID))
                {
                    usedElements.Add(element.ID);

                    if (element.hasTemplate())
                    {
                        if (!existTemplates.ContainsKey(element.Template.Name))
                        {
                            var superTable = TemplateSTableConverter.Convert(element.Template);
                            if (!superTable.HasValidColumn()) continue;
                            existTemplates.Add(superTable.Name, new List<AFElementWrapper>());
                            await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);
                            log.Info($"Creating TDengine super table info {element.Template.Name}");
                        }
                        existTemplates[element.Template.Name].Add(element);
                    }
                    else
                    {
                        var superTable = ElemenetSTableConverter.Convert(element);
                        if (!superTable.HasValidColumn()) return null;
                        await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);
                        log.Info($"Creating TDengine single super table info {element.Name}");
                    }
                }
            }

            log.Info($"Creating TDengine table start ...");
            int batchNum = 0;
            foreach (var template in existTemplates) {
                var superTableName = template.Key;
                var templateAttributeColumns = AttributeColumnConverter.Convert(template.Value.First().Template.AttributeTemplates);

                List<TDTable> tables = new List<TDTable>();
                foreach (var element in template.Value)
                {
                    TDTable table = ElemenetTableConverter.Convert(element, superTableName, templateAttributeColumns);
                    log.Debug($"Creating TDengine table for AF Element {element.Name} table: {table.Name}");
                    if (!elementsCollection.ContainsKey(table.Id))
                    {
                        tables.Add(table);
                        elementsCollection.Add(table.Id, element);
                    }
                    if (tables.Count() > 500) {
                        log.Info($"Creating TDengine table batch index: {++batchNum} st:{superTableName} ...");
                        await tdEngineProxy.CreateTablesForAFElementsV2(tdDatabaseName, superTableName, tables);
                        tables.Clear();
                    }
                };
                log.Info($"Creating TDengine table batch index: {++batchNum} st:{superTableName} ...");
                await tdEngineProxy.CreateTablesForAFElementsV2(tdDatabaseName, superTableName, tables);
                log.Info($"Creating TDengine table info for template: {superTableName} end");
            }
            return elementsCollection;
        }
        public async Task<Dictionary<string, AFElementWrapper>> CreateAFElementTablesByElementIds(string tdBase, string afDatabaseName)
        {
            if (0 == AppSettings.tomlConfig.ElementIDList.Count)
            {
                log.Info("No Element ID found.");
                return null;
            }
            return await GetElementsInfoByIds(tdBase, afDatabaseName, AppSettings.tomlConfig.ElementIDList);
        }

        public async Task<Dictionary<string, AFElementWrapper>> CreateAFElementTables(string tdDatabaseName, string afDatabaseName)
        {
            return await CreateAFElementTablesV2(tdDatabaseName, afDatabaseName);
        }
        public async Task<Dictionary<string, AFElementWrapper>> CreateAFElementTablesV2(string tdDatabaseName, string afDatabaseName)
        {
            if (AppSettings.tomlConfig.TemplateForAFElement.Count == 0
                && AppSettings.tomlConfig.ElementList.Count == 0)
            {
                log.Info("No TemplateForAFElement or Element found.");
                return null;
            }

            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName, AppSettings.tomlConfig.TemplateForAFElement).ToList();

            //get all AF Templates based on settings
            Dictionary<string, AFElementWrapper> elementsCollection = new Dictionary<string, AFElementWrapper>();

            foreach (AFElementTemplateWrapper elementTemplate in elementTemplates)
            {
                var elements = await CreateTaosxClientForElementTemplate(tdDatabaseName, elementTemplate);
                if (null == elements) continue;
                elementsCollection = elementsCollection.Concat(elements).ToDictionary(pair => pair.Key, pair => pair.Value);
            }
            foreach (string elementName in AppSettings.tomlConfig.ElementList) {
                var wrappers = piSystemManager.GetElementByName(afDatabaseName, elementName);
                foreach (AFElementWrapper element in wrappers)
                {
                    if (element.hasTemplate())
                    {
                        log.Error($"Element {elementName} is used for no template but it has template.");
                        continue;
                    }
                    var elements = await CreateTaosxClientForSingleElement(tdDatabaseName, element);
                    if (null == elements) continue;
                    elementsCollection = elementsCollection.Concat(elements).ToDictionary(pair => pair.Key, pair => pair.Value);
                }

            }
            return elementsCollection;
        }

        public async Task<Dictionary<string, AFElementWrapper>> CreateTaosxClientForElementTemplate(string tdDatabaseName, AFElementTemplateWrapper elementTemplate)
        {
            //check for associated supertable, create if needed
            var superTable = TemplateSTableConverter.Convert(elementTemplate);
            if (!superTable.HasValidColumn()) return null;
            await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

            //get all elements based on template
            IEnumerable<AFElementWrapper> elements = piSystemManager.GetElementTemplateInstances(elementTemplate);
            log.Info($"Found {elements.Count()} elements.");

            var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);

            Dictionary<string, AFElementWrapper> elementsCollection = new Dictionary<string, AFElementWrapper>();
            List<TDTable> tables = new List<TDTable>();
            foreach (var element in elements)
            {
                TDTable table = ElemenetTableConverter.Convert(element, superTable.Name, templateAttributeColumns);
                log.Debug($"Creating TDengine table for AF Element {element.Name} table: {table.Name}");
                if (!elementsCollection.ContainsKey(table.Name))
                {
                    tables.Add(table);
                    elementsCollection.Add(table.Name, element);
                }
            };
            await tdEngineProxy.CreateTablesForAFElementsV2(tdDatabaseName, superTable.Name, tables);
            return elementsCollection;
        }
        public async Task<bool> CreateOrUpdateSuperTables(string tdDatabaseName, AFElementTemplateWrapper elementTemplate)
        {
            TDEngineResponse res = new TDEngineResponse();
            bool hasNewAttribute = false;
            var superTable = TemplateSTableConverter.Convert(elementTemplate);
            try {
                res = await tdEngineProxy.GetSTables(tdDatabaseName, superTable.Name);
                hasNewAttribute = false;

            } catch (Exception e) {
                log.Error($"GetSTables failed.{e}");
                return false;
            }
            if (null == res || null == res.Data)
            {
                // Adding super tables at runtime is not supported
                return false;
            }
            else
            {
                var diff = GetTableChange(res.Data, superTable);
                log.Debug($"super table(old) {superTable.Name} columns:{string.Join(",", diff.OldColumns)} tag:{string.Join(",", diff.OldTags)}");
                log.Debug($"super table(new) {superTable.Name} columns:{string.Join(",", diff.NewColumns)} tag:{string.Join(",", diff.NewTags)}");
                var changes = diff.GetOperFromDiff();
                if (changes.Count > 0) {
                    log.Info($"Pi Template {superTable.Name} changed, restart taosxclient.");
                    RestartTaosxClient(tdDatabaseName, elementTemplate);
                }
                foreach (var change in changes)
                {
                    if (change.Contains("ADD"))
                    {
                        hasNewAttribute = true;
                    }
                }
            }
            return hasNewAttribute;
        }
        private void RestartTaosxClient(string tdDatabaseName, AFElementTemplateWrapper elementTemplate)
        {
            tdEngineProxy.StopTaosxClient(elementTemplate.Name);
            _ = CreateTaosxClientForElementTemplate(tdDatabaseName, elementTemplate);
            return;
        }
        public TableDiff GetTableChange(List<List<string>> tdResponseColumns, TDSTable superTable)
        {
            TableDiff diff = new TableDiff();
            var oldColumns = new Dictionary<string, string>();
            var oldTags = new Dictionary<string, string>();
            foreach (var col in tdResponseColumns)
            {
                var type = col[1] == "NCHAR" ? "NCHAR(" + col[2] + ")" : col[1];
                if (col[3] == "TAG")
                {
                    oldTags.Add(col[0], type);
                }
                else
                {
                    oldColumns.Add(col[0], type);
                }
            }
            var newColumns = new Dictionary<string, string>();
            var newTags = new Dictionary<string, string>();
            newTags.Add("element_id", "NCHAR(100)");
            newColumns.Add("ts", "TIMESTAMP");
            foreach (var col in superTable.Columns)
            {
                if (col.IsTDengineTag())
                {
                    newTags.Add(col.Name, "NCHAR(100)");
                }
                else
                {
                    newColumns.Add($"{col.Name}_val", col.Type);
                    newColumns.Add($"{col.Name}_status", "INT");
                }
            }
            newTags.Add($"{AppSettings.tomlConfig.AFTreeTagName}", "NCHAR(100)");
            diff.OldColumns = oldColumns;
            diff.OldTags = oldTags;
            diff.NewColumns = newColumns;
            diff.NewTags = newTags;
            return diff;
        }
        public async Task<Dictionary<string, AFElementWrapper>> CreateTaosxClientForSingleElement(string tdDatabaseName, AFElementWrapper element)
        {
            //check for associated supertable, create if needed
            var superTable = ElemenetSTableConverter.Convert(element);
            if (!superTable.HasValidColumn()) return null;
            await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

            var attributeColumns = AttributeColumnConverter.Convert(element.Attributes);

            Dictionary<string, AFElementWrapper> elementsCollection = new Dictionary<string, AFElementWrapper>();
            List<TDTable> tables = new List<TDTable>();

            TDTable table = ElemenetTableConverter.Convert(element, superTable.Name, attributeColumns);
            log.Debug($"Creating TDengine table for AF Element {element.Name} table: {table.Name}");
            if (!elementsCollection.ContainsKey(table.Name))
            {
                tables.Add(table);
                elementsCollection.Add(table.Name, element);
            }

            await tdEngineProxy.CreateTablesForAFElementsV2(tdDatabaseName, superTable.Name, tables);
            return elementsCollection;
        }
    }
    public class TableDiff
    {
        public Dictionary<string, string> OldColumns;
        public Dictionary<string, string> OldTags;
        public Dictionary<string, string> NewColumns;
        public Dictionary<string, string> NewTags;

        // | ADD COLUMN col_name column_type
        // | DROP COLUMN col_name
        // | MODIFY COLUMN col_name column_type
        // | ADD TAG tag_name tag_type
        // | DROP TAG tag_name
        // | MODIFY TAG tag_name tag_type
        // | RENAME TAG old_tag_name new_tag_name
        public List<string> GetOperFromDiff()
        {
            var columnAdd = new Dictionary<string, string>();
            var columnDel = new List<string>();
            var tagAdd = new Dictionary<string, string>();
            var tagDel = new List<string>();
            var columnModify = new Dictionary<string, string>();
            var tagModify = new Dictionary<string, string>();
            // columns todo 
            calChanges(OldColumns, NewColumns, ref columnAdd, ref columnDel, ref columnModify);
            calChanges(OldTags, NewTags, ref tagAdd, ref tagDel, ref tagModify);

            var res = new List<string>();
            foreach (var add in columnAdd)
            {
                res.Add($"ADD COLUMN {add.Key} {add.Value}");
            }
            foreach (var del in columnDel)
            {
                res.Add($"DROP COLUMN {del}");
            }
            foreach (var mod in columnModify)
            {
                res.Add($"MODIFY COLUMN {mod.Key} {mod.Value}");
            }
            foreach (var add in tagAdd)
            {
                res.Add($"ADD TAG {add.Key} {add.Value}");
            }
            foreach (var del in tagDel)
            {
                res.Add($"DROP TAG {del}");
            }
            foreach (var mod in tagModify)
            {
                res.Add($"MODIFY TAG {mod.Key} {mod.Value}");
            }
            return res;
        }
        private void calChanges(Dictionary<string, string> columns1, Dictionary<string, string> columns2,
            ref Dictionary<string, string> columnAdd, ref List<string> columnDel, ref Dictionary<string, string> columnModify)
        {
            foreach (var oc in columns1)
            {
                if (columns2.ContainsKey(oc.Key))
                {
                    if (oc.Value != columns2[oc.Key])
                    {
                        columnModify.Add(oc.Key, columns2[oc.Key]);
                    }
                }
                else
                {
                    columnDel.Add(oc.Key);
                }
            }
            foreach (var nc in columns2)
            {
                if (!columns1.ContainsKey(nc.Key))
                {
                    columnAdd.Add(nc.Key, nc.Value);
                }
            }
        }
    }
}

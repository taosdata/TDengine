using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Helper;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core
{
    public class TablesCreator
    {
        private readonly PISystemManager piSystemManager;
        private readonly PIServerManager piServerManager;
        private readonly TDEngineProxy tdEngineProxy;

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
        public async Task<List<TDTable>> CreatePIPointTables(string tdDatabaseName, string afDatabaseName, bool dropTableFirst = false)
        {
            var piPoints = new List<TDTable>();
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


            if (points.Count == 0)
            {
                log.Info("No points found.");
                return piPoints;
            }
            else
            {
                log.Info($"Found {points.Count()} PI Points.");
            }

            foreach (var point in points)
            {
                string tdColumnType = PointTypeConverter.Convert(point.PointType);
                string superTableName = TableNameConvert.GetPIPointSuperTableName(point);
                var table = new TDTable(point.Name, point.ID, superTableName, tdColumnType);
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

            List<string> STableNames = piPoints.Select(p => p.STableName.ToLower()).Distinct().ToList();
            foreach (string STableName in STableNames)
            {
                var piPoint = piPoints.Where(p => p.STableName.ToLower() == STableName.ToLower()).First();
                await tdEngineProxy.CreateSuperTableForPIPoint(tdDatabaseName, piPoint.STableName, piPoint.ColumnType);
            }

            await tdEngineProxy.CreateTablesForPIPoints(tdDatabaseName, piPoints);


            //check TD Engine for backfilling of points
            return piPoints;
        }

        public async Task<Dictionary<string, AFElementWrapper>> CreateAFElementTables(string tdDatabaseName, string afDatabaseName)
        {
            if (AppSettings.tomlConfig.TemplateForAFElement == null)
            {
                log.Info("No TemplateForAFElement found.");
                return null;
            }

            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName, AppSettings.tomlConfig.TemplateForAFElement).ToList();

            //get all AF Templates based on settings
            Dictionary<string, AFElementWrapper> elementsCollection = new Dictionary<string, AFElementWrapper>();

            List<TDTable> tables = new List<TDTable>();

            foreach (AFElementTemplateWrapper elementTemplate in elementTemplates)
            {
                //check for associated supertable, create if needed
                var superTable = TemplateSTableConverter.Convert(elementTemplate);
                await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

                //get all elements based on template
                IEnumerable<AFElementWrapper> elements = piSystemManager.GetElementTemplateInstances(elementTemplate);
                log.Info($"Found {elements.Count()} elements.");

                var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);

             
                foreach (var element in elements)
                {
                    TDTable table = ElemenetTableConverter.Convert(element, superTable.Name, templateAttributeColumns);
                    log.Debug($"Creating TDengine table for AF Element {element.Name} table: {table.Name}");
                    if (!elementsCollection.ContainsKey(table.Name)) {
                        tables.Add(table);
                        elementsCollection.Add(table.Name, element);
                    }
                };
            }
            await tdEngineProxy.CreateTablesForAFElements(tdDatabaseName, tables);
            return elementsCollection;
        }
        public async Task<bool> CreateOrUpdateSTablesByTem(string tdDatabaseName, AFElementTemplateWrapper elementTemplate)
        {
            var superTable = TemplateSTableConverter.Convert(elementTemplate);
            return await CreateOrUpdateSuperTables(tdDatabaseName, superTable);
        }
        public async Task<bool> CreateOrUpdateSuperTables(string tdDatabaseName, TDSTable superTable)
        {
            var res = await tdEngineProxy.GetSTables(tdDatabaseName, superTable.Name);
            var hasNewAttribute = false;
            if (res.Data == null)
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
                foreach (var change in changes)
                {
                    // TODO restart this super table client
                    if (change.Contains("ADD"))
                    {
                        hasNewAttribute = true;
                    }
                }
            }
            return hasNewAttribute;
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
                if (col.IsTag())
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
            // calChanges(OldColumns, NewColumns, ref columnAdd, ref columnDel, ref columnModify);
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

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
            if (AppSettings.Points != null)
            {
                points.AddRange(piServerManager.FindPIPoints(AppSettings.Points));
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


    }
}

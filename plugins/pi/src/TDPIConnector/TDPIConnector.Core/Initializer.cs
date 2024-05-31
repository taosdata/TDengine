using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OSIsoft.AF.Asset;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;
using System.Threading;
using TDPIConnector.Core.Tasks;
using System.Diagnostics;

namespace TDPIConnector.Core
{
    public class Initializer
    {
        private readonly PISystemManager piSystemManager;
        private readonly PIServerManager piServerManager;
        private readonly TDEngineProxy tdEngineProxy;
        private Backfill backfill;
        private EventsSenderTask eventsSenderTask;
        private ElementModeTask elementModeTask;

        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public Initializer(ref PISystemManager piSystemManager,ref  PIServerManager piServerManager, ref TDEngineProxy tdEngineProxy, ref ElementModeObserver elementModeObserver, ref EventsSender eventsSender, ref BackfillManager backfillManager)
        {
            this.piSystemManager = piSystemManager;
            this.piServerManager = piServerManager;
            this.tdEngineProxy = tdEngineProxy;
            this.backfill = backfillManager.GetBackfill();
            eventsSenderTask = new EventsSenderTask(eventsSender);
            eventsSenderTask.Start();
            elementModeTask = new ElementModeTask(piSystemManager, elementModeObserver);
            elementModeTask.Start();
        }

        public void InitTaskForElements(ref AFElementTemplateWrapper elementTemplate, ref TDSTable superTable, ref List<AFElementWrapper> elements, ref IEnumerable<TDColumn> templateAttributeColumns) {
            List<TDTable> tables = new List<TDTable>();
            List<AFAttribute> attries = new List<AFAttribute>();
            foreach (var element in elements) {
                TDTable table = ElemenetTableConverter.ConvertV2(element, superTable.Name, ref templateAttributeColumns, ref attries);
                tables.Add(table);
            }
            tdEngineProxy.CreateTablesForAFElementsV2(superTable.Name, tables);
            tables.Clear();
            if (AppSettings.tomlConfig.ForBackfill)
            {
                attries.Clear();
                backfill.BackfillElementsOfTemplate(elementTemplate.Name, elements, AppSettings.tomlConfig.BackfillStartTime.UtcDateTime,
                                AppSettings.tomlConfig.BackfillEndTime.UtcDateTime);
            }
            else
            {
                elementModeTask.SignUpBatchAttributes(elementTemplate.Name, ref attries);
                attries.Clear();
                if (AppSettings.tomlConfig.MaxBackfillRangeDays > 0)
                {
                    var backfillStartLimit = DateTime.UtcNow.AddMinutes(-AppSettings.tomlConfig.MaxBackfillRangeDays);
                    backfill.BackfillElementsOfTemplate(elementTemplate.Name, elements, backfillStartLimit, DateTime.UtcNow);
                    log.Info($"Backfill started successfully.");
                }
            }
        }

        public async Task AddNewElementToTaskAsync(AFElementWrapper element) {
            if (element.Template != null)
            {
                AFElementTemplateWrapper elementTemplate = element.Template;
                var superTable = TemplateSTableConverter.Convert(elementTemplate);
                if (!superTable.HasValidColumn()) return;

                var taosxClient = tdEngineProxy.GetTaosxClient(superTable.Name);
                if (taosxClient == null)
                {
                    await tdEngineProxy.CreateSuperTableForAFElement(AppSettings.tomlConfig.TDDataBase, superTable);
                    log.Info($"New Element add event， create super table finished.");
                }

                var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);
                var elements = new List<AFElementWrapper>() { element };
                InitTaskForElements(ref elementTemplate, ref superTable, ref elements, ref templateAttributeColumns);
            }
            else {
                log.Info($"New Element is not in any template, skip.");
            }
            return;
        }

        public async Task InitTaskForElementTemplate(string tdDatabaseName, AFElementTemplateWrapper elementTemplate)
        {
            //check for associated supertable, create if needed
            var superTable = TemplateSTableConverter.Convert(elementTemplate);
            if (!superTable.HasValidColumn()) return;
            await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

            //get all elements based on template
            List<AFElementWrapper> elements = piSystemManager.GetElementsByTemplate(AppSettings.tomlConfig.AFDatabaseName, elementTemplate.Name).ToList();
            log.Info($"Found {elements.Count()} elements in template:{elementTemplate.Name}.");

            int chunkSize = 500;
            List<List<AFElementWrapper>> chunks = new List<List<AFElementWrapper>>();
            for (int i = 0; i < elements.Count; i += chunkSize)
            {
                chunks.Add(elements.GetRange(i, Math.Min(chunkSize, elements.Count - i)));
            }

            var templateAttributeColumns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);

            List<Task> tasks = new List<Task>();
            int groups = 5;
            long finishedCount = 0;
            for (int i = 0; i < groups; ++i)
            {
                int groupIndex = i;
                tasks.Add(Task.Run(async () =>
                {
                    try {
                        Stopwatch stopwatch = new Stopwatch();
                        for (int j = groupIndex; j < chunks.Count(); j += groups)
                        {
                            stopwatch.Reset();
                            stopwatch.Start();
                            var elementChunk = chunks[j];
                            InitTaskForElements(ref elementTemplate, ref superTable, ref elementChunk, ref templateAttributeColumns);
                            Interlocked.Add(ref finishedCount, elementChunk.Count);
                            stopwatch.Stop();
                            TimeSpan elapsed = stopwatch.Elapsed;
                            log.Info($"Start(Init) info: {Interlocked.Read(ref finishedCount)}/{elements.Count()}" +
                                $" elements in template:{elementTemplate.Name} group:{groupIndex} cost time:{elapsed.TotalSeconds} seconds.");
                        }
                    }
                    catch (Exception e) {
                        log.Error($"InitTaskForElements Exception: {e.Message}");
                    }
                }));
            }
            Task.WaitAll(tasks.ToArray());
            log.Info($"Start info: {elements.Count()} elements in template:{elementTemplate.Name}.");
            elements.Clear();
            return;
        }

        public async Task CreateTaosxClientForSingleElement(string tdDatabaseName, AFElementWrapper element)
        {
            //check for associated supertable, create if needed
            var superTable = ElemenetSTableConverter.Convert(element);
            if (!superTable.HasValidColumn()) return;
            await tdEngineProxy.CreateSuperTableForAFElement(tdDatabaseName, superTable);

            var attributeColumns = AttributeColumnConverter.Convert(element.Attributes);

            List<TDTable> tables = new List<TDTable>();

            TDTable table = ElemenetTableConverter.Convert(element, superTable.Name, ref attributeColumns);
            log.Debug($"Creating TDengine table for AF Element {element.Name} table: {table.Name}");
            tables.Add(table);

            await tdEngineProxy.CreateTablesForAFElementsV2(superTable.Name, tables);
            return;
        }

        public async Task InitAFModeTask(string tdDatabaseName, string afDatabaseName)
        {
            if (AppSettings.tomlConfig.TemplateForAFElement.Count == 0)
            {
                log.Info("Not found any TemplateForAFElement.");
                return;
            }

            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName, AppSettings.tomlConfig.TemplateForAFElement).ToList();

            List<Task> tasks = new List<Task>();

            SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(5);
            foreach (AFElementTemplateWrapper elementTemplate in elementTemplates)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await concurrencySemaphore.WaitAsync();
                    try
                    {
                        await InitTaskForElementTemplate(tdDatabaseName, elementTemplate);
                        log.Info($"template {elementTemplate.Name} Init finished.");
                    }
                    catch (Exception e) {
                        log.Error($"InitTaskForElementTemplate Excepiton : {e.Message}");
                    }
                    finally
                    {
                        concurrencySemaphore.Release();
                    }
                }));
            }

            foreach (string elementName in AppSettings.tomlConfig.ElementList)
            {
                var wrappers = piSystemManager.GetElementByName(afDatabaseName, elementName);
                foreach (AFElementWrapper element in wrappers)
                {
                    if (element.hasTemplate())
                    {
                        log.Error($"Element {elementName} is used for no template but it has template.");
                        continue;
                    }
                    await CreateTaosxClientForSingleElement(tdDatabaseName, element);
                }

            }
            Task.WaitAll(tasks.ToArray());

            return;
        }
    }
}

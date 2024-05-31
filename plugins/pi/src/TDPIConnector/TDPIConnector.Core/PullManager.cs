using System;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Diagnostics;
using log4net;
using OSIsoft.AF.Asset;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Conversions;
using System.Collections.Concurrent;

namespace TDPIConnector.Core
{
    internal class PullManager
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PISystemManager piSystemManager;
        private PIServerManager piServerManager;
        private string afDatabaseName;
        private TDEngineProxy tdEngineProxy;
        private bool stopRequested;
        private readonly Task task;

        ConcurrentDictionary<Guid, Dictionary<Guid, AttributeValue>> tagValRecords;

        public PullManager(PISystemManager piSystemManager, PIServerManager piServerManager, TDEngineProxy tdEngineProxy)
        {
            this.piSystemManager = piSystemManager;
            this.piServerManager = piServerManager;
            this.tdEngineProxy = tdEngineProxy;
            afDatabaseName = AppSettings.tomlConfig.AFDatabaseName;
            const long interval = 30 * 60 * 1000;

            this.task = new Task(async () =>
            {
                Stopwatch stopwatch = new Stopwatch();
                while (!stopRequested)
                {
                    stopwatch.Reset();
                    log.Info("PullManager pull tag value start...");
                    try
                    {
                        RefreshTagValue();
                    }
                    catch (Exception e) {
                        log.Info($"Exception when RefreshTagValue: {e.Message}");
                        Thread.Sleep(100000);
                    }
                    long cost = stopwatch.ElapsedMilliseconds;
                    log.Info($"PullManager pull tag value finsh. cost: {cost}.");
                    if (cost < interval) {
                        Thread.Sleep((int)(interval - cost));
                    }
                }
            });
        }

        private void RefreshTagValue()
        {
            foreach (var elementRecord in tagValRecords) {
                var element = piSystemManager.GetElementsById(afDatabaseName, elementRecord.Key);
                foreach (var attr in element.Attributes) {
                    if (elementRecord.Value.ContainsKey(attr.Guid)) {
                        var value = elementRecord.Value[attr.Guid];
                        if (attr.GetType() != value.type) {
                            log.Info($"Element:{element.GetPath()} attribute:{attr.Name}. type changed from {value.type} to {attr.GetType()}.");
                        }  else {
                            var newValue = attr.GetValueString().Trim();
                            if(newValue != value.value)
                            {
                                log.Info($"Element:{element.GetPath()} attribute:{attr.Name}. value changed from {value.value} to {newValue}.");
                                var elementTbName = ElemenetTableConverter.GetTDTableNameForElement(element);
                                this.tdEngineProxy.ChangeTagValueForAFElements(AppSettings.tomlConfig.TDDataBase, elementTbName, attr.Name, newValue).Wait();
                            }
                        }
                    }
                }

            }
        }

        public void Start()
        {
            log.Debug("Starting PullManager...");
            this.task.Start();
            log.Debug("PullManager started successfully");
        }

        internal void addNewAttries(in List<AFAttribute> attries)
        {
            foreach (var attr in attries) {
                Guid elementID = attr.Element.ID;
                if (tagValRecords.ContainsKey(elementID))
                {
                }
                else {
                    if (tagValRecords[elementID].ContainsKey(attr.ID)) {
                        tagValRecords[elementID][attr.ID] = attr.
                    }
                }
            }
            throw new NotImplementedException();
        }

        public void Stop()
        {
            log.Debug("Stopping PullManager...");
            this.stopRequested = true;
            this.task.Wait();
            log.Debug("PullManager stopped successfully");
        }

        private class AttributeValue
        {
            public Type type;
            public string value;
        }
    }
}
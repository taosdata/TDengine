using log4net;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;
using System.Threading;
using System.Threading.Tasks;

namespace TDPIConnector.Core
{
    public class EventsSender
    {
        private readonly TDEngineProxy tdEngineProxy;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpPIEvents;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpAFEvents;
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public event EventHandler<List<AFDataPipeEventWrapper>> OnPIEventReceivedListSuccess = delegate { };
        public event EventHandler<List<AFDataPipeEventWrapper>> OnAFEventReceivedListSuccess = delegate { };

        public event EventHandler<AFDataPipeEventWrapper> OnPIEventReceivedSuccess = delegate { };
        public event EventHandler<AFDataPipeEventWrapper> OnAFEventReceivedSuccess = delegate { };

        RangeDeleteEventsSender deleteSender;

        public EventsSender(TDEngineProxy tdEngineProxy)
        {
            this.tdEngineProxy = tdEngineProxy;
            this.dpPIEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
            this.dpAFEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
            deleteSender = new RangeDeleteEventsSender(tdEngineProxy, null);
        }

        public void SetBackfill(BackfillManager backfill)
        {
            deleteSender.SetBackfill(backfill);
        }

        public void OnAFElementEventBatch(ref List<AFDataPipeEventWrapper> allEvents) {
            this.OnAFEventReceivedListSuccess(this, allEvents.Take(100).ToList());

            var stables = new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
            var columnNames = new List<string>();

            foreach (var dpEvent in allEvents)
            {
                string stableName;
                if (!dpEvent.Value.Attribute.Element.hasTemplate())
                {
                    stableName = TableNameConvert.GetSingleElementSuperTableName(dpEvent.Value.Attribute.Element);
                }
                else
                {
                    stableName = TableNameConvert.GetAFPointSuperTableName(dpEvent.Value.Attribute.Element.Template);
                }
                var elementTbName = ElemenetTableConverter.GetTDTableNameForElement(dpEvent.Value.Attribute.Element);
                var elementUniKey = dpEvent.Value.Attribute.Element.ID.ToString();
                var tdValue = dpEvent.Value.ToTDValue();
                if (tdValue == null) continue;
                var timestamp = tdValue.TimestampString;

                var attributeName = dpEvent.Value.Attribute.Name;
                if (dpEvent.IsAFDataPipeRangeDeletedEvent())
                {
                    var rangeDeleteEvent = dpEvent.ToAFDataPipeRangeDeletedEventWrapper();
                    var startTime = rangeDeleteEvent.StartTime;
                    var endTime = rangeDeleteEvent.EndTime;
                    deleteSender.AddDeleteRange(dpEvent.Value.Attribute.Element, startTime, endTime);
                    log.Debug($"element range delete event {elementTbName}:{attributeName} {startTime.LocalTime}-{endTime.LocalTime}");
                    continue;
                }
                if (dpEvent.Value.Attribute.IsTDengineTag())
                {
                    if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Update ||
                        dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Add)
                    {
                        var valueString = dpEvent.Value.Attribute.ToStringWithUOM();
                        log.Info($"element tag change {elementTbName}:{attributeName}:{valueString}");
                        this.tdEngineProxy.ChangeTagValueForAFElements(AppSettings.tomlConfig.TDDataBase, elementTbName, attributeName, valueString).Wait();
                    }
                    continue;
                }

                if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Delete &&
                    !dpEvent.Value.Attribute.Unsupported())
                {
                    log.Debug($"element event delete {elementTbName}:{attributeName}:{timestamp}");
                    tdValue.SetTDDeleted();
                }
                if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Refresh && !dpEvent.Value.Attribute.Unsupported())
                {
                    // log.Info($"element event refresh {elementName}:{attributeName}:{timestamp}");
                    continue;
                }
                if (!columnNames.Contains(attributeName))
                {
                    columnNames.Add(attributeName);
                }

                if (dpEvent.Value.Attribute.IsChild()) {
                    tdValue.Name = AttributeColumnConverter.GetChildAttrbuteName(dpEvent.Value.Attribute.Parent, dpEvent.Value.Attribute);
                } else {
                    tdValue.Name = dpEvent.Value.Attribute.Name;
                }

                if (stables.ContainsKey(stableName))
                {
                    if (stables[stableName].ContainsKey(elementUniKey))
                    {
                        var table = stables[stableName][elementUniKey];
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
                        stables[stableName].Add(elementUniKey, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                    }
                }
                else
                {
                    var tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();
                    tables.Add(elementUniKey, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                    stables.Add(stableName, tables);
                }
            }
            log.Info($"Element mode events {allEvents.Count}");
            try
            {
                this.tdEngineProxy.InsertValuesForAFElements(AppSettings.tomlConfig.TDDataBase, stables, columnNames).Wait();
            }
            catch (Exception e)
            {
                throw e.InnerException;
            }
        }

        public void OnAFElementEvents()
        {
            if (StandbyManager.Instance.StandByModeEnabled)
            {
                return;
            }

            List<AFDataPipeEventWrapper> allEvents = new List<AFDataPipeEventWrapper>();

            while (!this.dpAFEvents.IsEmpty)
            {
                bool result = this.dpAFEvents.TryDequeue(out AFDataPipeEventWrapper dpEvent);
                if (result)
                {
                    allEvents.Add(dpEvent);
                    this.OnAFEventReceivedSuccess(this, dpEvent);
                }
                if (allEvents.Count >= 10000) {
                    OnAFElementEventBatch(ref allEvents);
                    allEvents.Clear();
                }
            }
            if (allEvents.Count == 0)
            {
                return;
            }

            OnAFElementEventBatch(ref allEvents);
        }

        public void OnPIPointEvents()
        {
            if (StandbyManager.Instance.StandByModeEnabled)
            {
                return;
            }

            List<AFDataPipeEventWrapper> allEvents = new List<AFDataPipeEventWrapper>();

            while (!this.dpPIEvents.IsEmpty)
            {
                bool result = this.dpPIEvents.TryDequeue(out AFDataPipeEventWrapper dpEvent);
                if (result)
                {
                    allEvents.Add(dpEvent);

                    this.OnPIEventReceivedSuccess(this, dpEvent);

                }
            }

            if (allEvents.Count == 0)
            {
                return;
            }

            this.OnPIEventReceivedListSuccess(this, allEvents.Take(100).ToList());
            foreach (var dpEvent in allEvents)
            {
                var superTableName = PIInfoScanner.GeneratePointSuperTableName(dpEvent.Value.PIPoint);
                var tdValue = dpEvent.Value.ToTDValue();
                if (tdValue == null) continue;
                var timestamp = tdValue.TimestampString;
                tdValue.Name = dpEvent.Value.PIPoint.Name;

                try
                {
                    this.tdEngineProxy.InsertValueForPIPoints(superTableName, tdValue);
                }
                catch (Exception e)
                {
                    throw e.InnerException;
                }
            }
        }

        internal void AddPIValue(AFDataPipeEventWrapper dpEvent)
        {
            this.dpPIEvents.Enqueue(dpEvent);
        }

        internal void AddAFValue(AFDataPipeEventWrapper dpEvent)
        {
            this.dpAFEvents.Enqueue(dpEvent);
        }
    }

    class RangeDelete {
        public RangeDelete(AFElementWrapper element, AFTimeWrapper startTime, AFTimeWrapper endTime) {
            this.element = element;
            StartTime = startTime;
            EndTime = endTime;
        }
        public AFElementWrapper element;
        public AFTimeWrapper StartTime;
        public AFTimeWrapper EndTime;
    }

    class RangeDeleteEventsSender {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly TDEngineProxy tdProxy;
        private BackfillManager backfillManager;

        public Dictionary<string, RangeDelete> deleteElements = new Dictionary<string, RangeDelete> { }; // key: string elementName;
        private readonly Object stLock = new Object();
        public DateTime LastUpdataTime;
        public bool startBackfill = false;

        public RangeDeleteEventsSender(TDEngineProxy tdProxy, BackfillManager backfillManager)
        {
            this.tdProxy = tdProxy;
            this.backfillManager = backfillManager;
        }
        public void AddDeleteRange(AFElementWrapper element, AFTimeWrapper startTime, AFTimeWrapper endTime) {
            lock (stLock) {
                var tbName = ElemenetTableConverter.GetTDTableNameForElement(element);
                if (deleteElements.ContainsKey(tbName))
                {
                    deleteElements[tbName].EndTime =
                        deleteElements[tbName].EndTime > endTime ? deleteElements[tbName].EndTime : endTime;
                    deleteElements[tbName].StartTime =
                        deleteElements[tbName].StartTime < startTime ? deleteElements[tbName].StartTime : startTime;
                }
                else
                {
                    var range = new RangeDelete(element, startTime, endTime);
                    deleteElements.Add(tbName, range);
                }
                LastUpdataTime = DateTime.Now;
                if (!startBackfill) {
                    startBackfill = true;
                    Task task = Task.Run(async () =>
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5));
                        Send();
                    });
                }
            }
            return;
        }

        public void Send()
        {
            while (true) {
                if (DateTime.Now < LastUpdataTime.AddSeconds(30)) {
                    Thread.Sleep(1000);
                    continue;
                }
                lock (stLock)
                {
                    foreach (var element in deleteElements)
                    {
                        tdProxy.DeleteByTimeRange(AppSettings.tomlConfig.TDDataBase, element.Key,
                            element.Value.StartTime.FormatUtcTime(),
                            element.Value.EndTime.FormatUtcTime()).Wait();
                        log.Info($"element range delete event after merge {element.Key}:{element.Value.StartTime.LocalTime}-{element.Value.EndTime.LocalTime}");
                    }
                    Thread.Sleep(1000);
                    Backfill();
                    startBackfill = false;
                }
                break;
            }
        }
        public void Backfill()
        {
            if (backfillManager == null) {
                log.Info($"backfill in range delete sender is null");
                return;
            }
            lock (stLock)
            {
                foreach (var element in deleteElements)
                {
                    backfillManager.GetBackfill().BackfillElement(AppSettings.tomlConfig.TDDataBase, element.Value.element, element.Value.StartTime.UtcTime, element.Value.EndTime.UtcTime);
                    log.Info($"element {element.Key} refresh time range in:{element.Value.StartTime.LocalTime}-{element.Value.EndTime.LocalTime}");
                }
                deleteElements.Clear();
            }
        }

        public void SetBackfill(BackfillManager backfillManager) {
            this.backfillManager = backfillManager;
        }
    }
}

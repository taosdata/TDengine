using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Helper;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core
{
    public class EventsSender
    {
        private readonly TDEngineProxy tdEngineProxy;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpPIEvents;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpAFEvents;
        private static readonly ILog log = LogManager.GetLogger(
            System.Reflection.MethodBase.GetCurrentMethod().DeclaringType
        );

        public event EventHandler<List<AFDataPipeEventWrapper>> OnPIEventReceivedListSuccess =
            delegate { };
        public event EventHandler<List<AFDataPipeEventWrapper>> OnAFEventReceivedListSuccess =
            delegate { };

        public event EventHandler<AFDataPipeEventWrapper> OnPIEventReceivedSuccess = delegate { };
        public event EventHandler<AFDataPipeEventWrapper> OnAFEventReceivedSuccess = delegate { };

        List<AFDataPipeEventWrapper> allEvents = new List<AFDataPipeEventWrapper>();
        RangeDeleteEventsSender deleteSender;
        private readonly bool SyncDeleteData;
        private readonly bool SyncUpdateData;
        private readonly bool SyncUpdateAttribute;

        public EventsSender(TDEngineProxy tdEngineProxy)
        {
            this.tdEngineProxy = tdEngineProxy;
            dpPIEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
            dpAFEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
            deleteSender = new RangeDeleteEventsSender(tdEngineProxy, null);
            SyncDeleteData = AppSettings.tomlConfig.SyncDeleteData;
            SyncUpdateData = AppSettings.tomlConfig.SyncUpdateData;
            SyncUpdateAttribute = AppSettings.tomlConfig.SyncUpdateAttribute;
        }

        public void SetBackfill(BackfillManager backfill)
        {
            deleteSender.SetBackfill(backfill);
        }

        public void OnAFElementEventBatch(ref List<AFDataPipeEventWrapper> allEvents)
        {
            OnAFEventReceivedListSuccess(this, allEvents.Take(100).ToList());
            log.Debug($"AFElementEventBatch:{allEvents.Count}");
            var stables =
                new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
            var columnNames = new List<string>();

            foreach (var dpEvent in allEvents)
            {
                try
                {
                    OSIsoft.AF.Data.AFDataPipeAction action = dpEvent.AFEventAction();
                    string stableName;
                    if (dpEvent.Value.Attribute.Element != null)
                    {
                        if (!dpEvent.Value.Attribute.Element.hasTemplate())
                        {
                            stableName = TableNameConvert.GetSingleElementSuperTableName(
                                dpEvent.Value.Attribute.Element
                            );
                        }
                        else
                        {
                            stableName = TableNameConvert.GetAFPointSuperTableName(
                                dpEvent.Value.Attribute.Element.Template
                            );
                        }
                    }
                    else
                    {
                        log.Error(
                            $"DataPipeEvent-{action}:{dpEvent.Value.Attribute.Name} has no element"
                        );
                        continue;
                    }

                    var elementId = dpEvent.Value.Attribute.Element.ID.ToString();
                    var elementName = dpEvent.Value.Attribute.Element.Name;
                    var attributeName = dpEvent.Value.Attribute.Name;
                    var tdValue = dpEvent.Value.ToTDValue();
                    if (tdValue == null)
                    {
                        log.Warn(
                            $"DataPipeEvent-{action}:{dpEvent.Value.Attribute.Name}, ToTDValue failed"
                        );
                        continue;
                    }
                    if (dpEvent.Value.Attribute.IsChild())
                    {
                        // 计算子属性的列名
                        tdValue.Name = AttributeColumnConverter.GetChildAttrbuteName(
                            dpEvent.Value.Attribute.Parent,
                            dpEvent.Value.Attribute
                        );
                    }
                    else
                    {
                        tdValue.Name = dpEvent.Value.Attribute.Name;
                    }
                    var timestamp = tdValue.TimestampString;
                    bool isTDTag = dpEvent.Value.Attribute.IsTDengineTag();
                    // 更新历史数据(Update)、插入新数据(Add)、插入历史数据(Insert)
                    if (
                        (
                            action == OSIsoft.AF.Data.AFDataPipeAction.Add
                            || action == OSIsoft.AF.Data.AFDataPipeAction.Insert
                            || action == OSIsoft.AF.Data.AFDataPipeAction.Update
                        ) && !isTDTag
                    )
                    {
                        if (
                            action == OSIsoft.AF.Data.AFDataPipeAction.Update
                            || action == OSIsoft.AF.Data.AFDataPipeAction.Insert
                        )
                        {
                            log.Info(
                                $"DataPipeEvent-{action}:{stableName}:{elementName}_{elementId}:{attributeName}:{timestamp}"
                            );
                            if (
                                action == OSIsoft.AF.Data.AFDataPipeAction.Update
                                && !SyncUpdateData
                            )
                            {
                                log.Info($"DataPipeEvent-{action}:ignore update event");
                                continue;
                            }
                        }
                        if (!columnNames.Contains(attributeName))
                        {
                            columnNames.Add(attributeName);
                        }

                        if (stables.ContainsKey(stableName))
                        {
                            if (stables[stableName].ContainsKey(elementId))
                            {
                                var table = stables[stableName][elementId];
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
                                stables[stableName]
                                    .Add(
                                        elementId,
                                        new Dictionary<string, List<TDValue>>()
                                        {
                                            {
                                                timestamp,
                                                new List<TDValue>() { tdValue }
                                            }
                                        }
                                    );
                            }
                        }
                        else
                        {
                            var tables = new Dictionary<string, Dictionary<string, List<TDValue>>>
                            {
                                {
                                    elementId,
                                    new Dictionary<string, List<TDValue>>()
                                    {
                                        {
                                            timestamp,
                                            new List<TDValue>() { tdValue }
                                        }
                                    }
                                }
                            };
                            stables.Add(stableName, tables);
                        }
                        continue;
                    }
                    // 日志记录所有其它事件
                    log.Info(
                        $"DataPipeEvent-{action}:{stableName}:{elementName}_{elementId}:{attributeName}:{timestamp}"
                    );

                    // 处理 TAG 列变化
                    if (isTDTag)
                    {
                        if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Update)
                        {
                            if (!SyncUpdateAttribute)
                            {
                                log.Info($"DataPipeEvent-{action}:ignore update tag event");
                                continue;
                            }
                            var valueString = dpEvent.Value.Attribute.ToStringWithUOM();
                            log.Info(
                                $"DataPipeEvent-{action}:Tag change,{stableName}:{elementName}_{elementId}:{attributeName}:{valueString}"
                            );
                            tdEngineProxy.ChangeTagValueForAFElements(
                                stableName,
                                elementId,
                                tdValue.Name,
                                valueString.Trim()
                            );
                        }
                        else
                        {
                            log.Debug($"DataPipeEvent-{action}:ignore other tag event");
                        }
                        continue;
                    }
                    // 删除历史数据
                    if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Delete)
                    {
                        if (!SyncDeleteData)
                        {
                            log.Info($"DataPipeEvent-{action}:ignore delete event");
                            continue;
                        }
                        if (dpEvent.Value.Attribute.Unsupported())
                        {
                            log.Info(
                                $"DataPipeEvent-{action}:ignore unsupported datarefrence type"
                            );
                        }
                        else
                        {
                            // 更新属性值为 null
                            tdEngineProxy.DeleteByTime(
                                stableName,
                                elementId,
                                tdValue.Name.ToTDEngineNamingPattern(),
                                timestamp
                            );
                        }
                        continue;
                    }
                    // Analyses 重计算事件
                    if (dpEvent.IsAFDataPipeRangeDeletedEvent())
                    {
                        var rangeDeleteEvent = dpEvent.ToAFDataPipeRangeDeletedEventWrapper();
                        var startTime = rangeDeleteEvent.StartTime;
                        var endTime = rangeDeleteEvent.EndTime;
                        log.Info(
                            $"DataPipeRangeDeletedEvent:{stableName}:{elementName}_{elementId}:{attributeName},{startTime.LocalTime}-{endTime.LocalTime}"
                        );
                        deleteSender.AddDeleteRange(
                            stableName,
                            dpEvent.Value.Attribute.Element,
                            startTime,
                            endTime
                        );
                        continue;
                    }
                    // 处理刷新事件
                    if (dpEvent.AFEventAction() == OSIsoft.AF.Data.AFDataPipeAction.Refresh)
                    {
                        log.Info($"DataPipeEvent-{action}:ignore refresh event");
                        continue;
                    }
                    // 处理其它事件, 理论不应该走到这里，如果走到这里，需要检查代码逻辑
                    log.Warn($"DataPipeEvent-{action}:ignored");
                }
                catch (InvalidOperationException oe) { 
                    log.Warn($"Process DataPipeEvent Warn: {oe.Message}");
                }
                catch (Exception e)
                {
                    log.Error("Process DataPipeEvent Error", e);
                }
            }
            try
            {
                tdEngineProxy.InsertValuesForAFElements(stables, columnNames).Wait();
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

            while (!dpAFEvents.IsEmpty)
            {
                bool result = dpAFEvents.TryDequeue(out AFDataPipeEventWrapper dpEvent);
                if (result)
                {
                    allEvents.Add(dpEvent);
                    this.OnAFEventReceivedSuccess(this, dpEvent);
                }
                if (allEvents.Count >= 10000)
                {
                    OnAFElementEventBatch(ref allEvents);
                    allEvents.Clear();
                }
            }
            if (allEvents.Count == 0)
            {
                return;
            }

            OnAFElementEventBatch(ref allEvents);
            allEvents.Clear();
        }

        public void OnPIPointEvents()
        {
            if (StandbyManager.Instance.StandByModeEnabled)
            {
                return;
            }

            while (!dpPIEvents.IsEmpty)
            {
                bool result = dpPIEvents.TryDequeue(out AFDataPipeEventWrapper dpEvent);
                if (result)
                {
                    allEvents.Add(dpEvent);

                    OnPIEventReceivedSuccess(this, dpEvent);
                }
            }

            if (allEvents.Count == 0)
            {
                return;
            }

            OnPIEventReceivedListSuccess(this, allEvents.Take(100).ToList());
            foreach (var dpEvent in allEvents)
            {
                var superTableName = PIInfoScanner.GeneratePointSuperTableName(
                    dpEvent.Value.PIPoint
                );
                var tdValue = dpEvent.Value.ToTDValue();
                if (tdValue == null)
                    continue;
                var timestamp = tdValue.TimestampString;
                tdValue.Name = dpEvent.Value.PIPoint.Name;

                try
                {
                    tdEngineProxy.InsertValueForPIPoints(superTableName, tdValue);
                }
                catch (Exception e)
                {
                    throw e.InnerException;
                }
            }
            allEvents.Clear();
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

    class RangeDelete
    {
        public RangeDelete(
            AFElementWrapper element,
            string stableName,
            AFTimeWrapper startTime,
            AFTimeWrapper endTime
        )
        {
            this.element = element;
            StableName = stableName;
            StartTime = startTime;
            EndTime = endTime;
        }

        public AFElementWrapper element;
        public string StableName;
        public AFTimeWrapper StartTime;
        public AFTimeWrapper EndTime;
    }

    class RangeDeleteEventsSender
    {
        private static readonly ILog log = LogManager.GetLogger(
            System.Reflection.MethodBase.GetCurrentMethod().DeclaringType
        );
        private readonly TDEngineProxy tdProxy;
        private BackfillManager backfillManager;

        public Dictionary<string, RangeDelete> timeRangesToDelete = new Dictionary<
            string,
            RangeDelete
        >
        {
            }; // key: string elementName;
        private readonly Object stLock = new Object();
        public DateTime LastUpdataTime;
        public bool startBackfill = false;

        public RangeDeleteEventsSender(TDEngineProxy tdProxy, BackfillManager backfillManager)
        {
            this.tdProxy = tdProxy;
            this.backfillManager = backfillManager;
        }

        public void AddDeleteRange(
            string stableName,
            AFElementWrapper element,
            AFTimeWrapper startTime,
            AFTimeWrapper endTime
        )
        {
            lock (stLock)
            {
                var elementId = element.ID.ToString();
                if (timeRangesToDelete.ContainsKey(elementId))
                {
                    timeRangesToDelete[elementId].EndTime =
                        timeRangesToDelete[elementId].EndTime > endTime
                            ? timeRangesToDelete[elementId].EndTime
                            : endTime;
                    timeRangesToDelete[elementId].StartTime =
                        timeRangesToDelete[elementId].StartTime < startTime
                            ? timeRangesToDelete[elementId].StartTime
                            : startTime;
                }
                else
                {
                    var range = new RangeDelete(element, stableName, startTime, endTime);
                    timeRangesToDelete.Add(elementId, range);
                }
                LastUpdataTime = DateTime.Now;
                if (!startBackfill)
                {
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
            while (true)
            {
                if (DateTime.Now < LastUpdataTime.AddSeconds(30))
                {
                    Thread.Sleep(1000);
                    continue;
                }
                lock (stLock)
                {
                    foreach (var kv in timeRangesToDelete)
                    {
                        string elementId = kv.Key;
                        string elementName = kv.Value.element.Name;
                        string superTableName = kv.Value.StableName;
                        string startTime = kv.Value.StartTime.FormatUtcTime();
                        string endTime = kv.Value.EndTime.FormatUtcTime();
                        tdProxy.DeleteByTimeRange(superTableName, elementId, startTime, endTime);
                        log.Info(
                            $"Delete:{superTableName}:{elementName}_{elementId}:{kv.Value.StartTime.LocalTime}-{kv.Value.EndTime.LocalTime}"
                        );
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
            if (backfillManager == null)
            {
                log.Info($"Backfill in range delete sender is null");
                return;
            }
            lock (stLock)
            {
                foreach (var kv in timeRangesToDelete)
                {
                    try
                    {
                        backfillManager
                            .GetBackfill()
                            .BackfillElement(
                                AppSettings.tomlConfig.TDDataBase,
                                kv.Value.element,
                                kv.Value.StartTime.UtcTime,
                                kv.Value.EndTime.UtcTime
                            );
                        log.Info(
                            $"Backfill element {kv.Key} refresh time range in:{kv.Value.StartTime.LocalTime}-{kv.Value.EndTime.LocalTime}"
                        );
                    }
                    catch (Exception e)
                    {
                        log.Error(
                            $"Backfill element {kv.Key} refresh time range in:{kv.Value.StartTime.LocalTime}-{kv.Value.EndTime.LocalTime} error",
                            e
                        );
                    }
                }
                timeRangesToDelete.Clear();
            }
        }

        public void SetBackfill(BackfillManager backfillManager)
        {
            this.backfillManager = backfillManager;
        }
    }
}

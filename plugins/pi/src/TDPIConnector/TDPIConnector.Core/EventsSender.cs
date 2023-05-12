using log4net;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using TDPIConnector.Core.Conversions;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core
{
    class EventsSender
    {
        private readonly TDEngineProxy tdEngineProxy;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpPIEvents;
        private readonly ConcurrentQueue<AFDataPipeEventWrapper> dpAFEvents;
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public event EventHandler<List<AFDataPipeEventWrapper>> OnPIEventReceivedListSuccess = delegate { };
        public event EventHandler<List<AFDataPipeEventWrapper>> OnAFEventReceivedListSuccess = delegate { };

        public event EventHandler<AFDataPipeEventWrapper> OnPIEventReceivedSuccess = delegate { };
        public event EventHandler<AFDataPipeEventWrapper> OnAFEventReceivedSuccess = delegate { };

        public EventsSender(TDEngineProxy tdEngineProxy)
        {
            this.tdEngineProxy = tdEngineProxy;
            this.dpPIEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
            this.dpAFEvents = new ConcurrentQueue<AFDataPipeEventWrapper>();
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
            }

            if (allEvents.Count == 0)
            {
                return;
            }

            this.OnAFEventReceivedListSuccess(this, allEvents.Take(100).ToList());

            var stables = new Dictionary<string, Dictionary<string, Dictionary<string, List<TDValue>>>>();
            var columnNames = new List<string>();

            foreach (var dpEvent in allEvents)
            {
                var elementName = dpEvent.Value.Attribute.Element.Name;
                var stableName = TableNameConvert.GetAFPointSuperTableName(dpEvent.Value.Attribute.Element.Template);
                var tdValue = dpEvent.Value.ToTDValue();
                if (tdValue == null) continue;
                var timestamp = tdValue.TimestampString;

                var attributeName = dpEvent.Value.Attribute.Name;
                if (!columnNames.Contains(attributeName))
                {
                    columnNames.Add(attributeName);
                }

                tdValue.Name = dpEvent.Value.Attribute.Name;

                if (stables.ContainsKey(stableName))
                {
                    if (stables[stableName].ContainsKey(elementName))
                    {
                        var table = stables[stableName][elementName];
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
                        stables[stableName].Add(elementName, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                    }
                }
                else {
                    var tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();
                    tables.Add(elementName, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                    stables.Add(stableName, tables);
                }
            }
            log.Info($"Element mode events {allEvents.Count}");
            try
            {
                this.tdEngineProxy.InsertValuesForAFElements(AppSettings.tomlConfig.TDDataBase, stables, columnNames).Wait();
            }
            catch(Exception e)
            {
                throw e.InnerException;
            }

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
            var tables = new Dictionary<string, Dictionary<string, List<TDValue>>>();

            foreach (var dpEvent in allEvents)
            {
                var pointName = dpEvent.Value.PIPoint.Name;
                var tdValue = dpEvent.Value.ToTDValue();
                if (tdValue == null) continue;
                var timestamp = tdValue.TimestampString;

                tdValue.Name = pointName;

                if (tables.ContainsKey(pointName))
                {
                    var table = tables[pointName];
                    if (table.ContainsKey(timestamp))
                    {
                        // not support different value at the same one timestamp, use the last one.
                        table[timestamp] = new List<TDValue>() { tdValue };
                    }
                    else
                    {
                        table.Add(timestamp, new List<TDValue>() { tdValue });
                    }
                }
                else
                {
                    tables.Add(pointName, new Dictionary<string, List<TDValue>>() { { timestamp, new List<TDValue>() { tdValue } } });
                }
            }

            log.Info($"Point mode events: {allEvents.Count}");
            try
            {
                this.tdEngineProxy.InsertValuesForPIPoints(AppSettings.tomlConfig.TDDataBase, tables).Wait();
            }
            catch(Exception e)
            {
                throw e.InnerException;
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
}

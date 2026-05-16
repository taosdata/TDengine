using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using TDPIConnector.Core.Monitoring.Models;
using TDPIConnector.PI;
using TDPIConnector.PI.Exceptions;
using TDPIConnector.TDEngine.Exceptions;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Monitoring
{
    public class MonitoringService : IMonitoringService
    {
        private ConcurrentQueue<PIEventReceived> lastPIEvents;
        private ConcurrentQueue<AFEventReceived> lastAFEvents;
        private ConcurrentQueue<TDEngineHttpResponseSummary> tdEngineHttpResponses;
        private readonly IDictionary<string, int> eventsPerAttribute;
        private readonly IDictionary<string, int> eventsPerPoint;
        private readonly IDictionary<string, int> responsesPerCode;
        private readonly List<Exception> piExceptions;
        private readonly List<Exception> tdExceptions;    
        private readonly StandbyManager standbyManager;
        private PIConnection piConnectionInfo;
    
        private string tdEngineVersion;

        public bool Enabled { get; set; }

        public MonitoringService()
        {
            this.lastPIEvents = new ConcurrentQueue<PIEventReceived>();
            this.lastAFEvents = new ConcurrentQueue<AFEventReceived>();
            this.tdEngineHttpResponses = new ConcurrentQueue<TDEngineHttpResponseSummary>();
            this.eventsPerAttribute = new Dictionary<string, int>();
            this.eventsPerPoint = new Dictionary<string, int>();
            this.responsesPerCode = new Dictionary<string, int>();
            this.piExceptions = new List<Exception>();
            this.tdExceptions = new List<Exception>();
            this.standbyManager = StandbyManager.Instance;
        }

        public void PublishTDEngineHttpResponse(TDEngineHttpResponseSummary httpResponse)
        {
            if (!Enabled)
            {
                return;
            }
            httpResponse.Timestamp = DateTime.Now;
            tdEngineHttpResponses.Enqueue(httpResponse);

            if (AppSettings.WebMaxTDEngineHttpResponses > 0 && tdEngineHttpResponses.Count > AppSettings.WebMaxTDEngineHttpResponses)
            {
                var r = tdEngineHttpResponses.TryDequeue(out TDEngineHttpResponseSummary tdEngineHttpResponseSummary);
            }
            string key = $"{httpResponse.HttpStatusCode}-{httpResponse.TDEngineCode}";
            if (!responsesPerCode.ContainsKey(key))
            {
                this.responsesPerCode[key] = 0;
            }
            this.responsesPerCode[key]++;
            standbyManager.ReportTDEngineConnectionSuccess();
        }

        public void PublishPIEvent(AFDataPipeEventWrapper dpEvent)
        {
            if (!Enabled)
            {
                return;
            }
            if (dpEvent.Value.PIPoint != null)
            {
                string pointName = dpEvent.Value.PIPoint.Name;
                if (!eventsPerPoint.ContainsKey(pointName) && eventsPerAttribute.Keys.Count < 1000)
                {
                    this.eventsPerPoint[dpEvent.Value.PIPoint.Name] = 0;
                }
                if (eventsPerPoint.ContainsKey(pointName)) {
                    this.eventsPerPoint[dpEvent.Value.PIPoint.Name]++;
                }
            }
        }

        public void PublishAFEvent(AFDataPipeEventWrapper dpEvent)
        {
            if (!Enabled)
            {
                return;
            }
            if (dpEvent.Value.Attribute != null)
            {
                string attributePath = dpEvent.Value.Attribute.GetPath();
                if (!eventsPerAttribute.ContainsKey(attributePath) && eventsPerAttribute.Keys.Count < 1000)
                {
                    this.eventsPerAttribute[attributePath] = 0;
                }
                if (eventsPerAttribute.ContainsKey(attributePath))
                {
                    this.eventsPerAttribute[attributePath]++;
                }
            }
        }

        public void PublishLastPIEvent(List<AFDataPipeEventWrapper> dpEvents)
        {
            if (!Enabled)
            {
                return;
            }
            foreach (var dpEvent in dpEvents)
            {
                if (dpEvent.Value.PIPoint != null)
                {
                    PIEventReceived eventReceived = new PIEventReceived()
                    {
                        Point = dpEvent.Value.PIPoint.Name,
                        Value = dpEvent.Value.Value,
                        Timestamp = dpEvent.Value.Timestamp.UtcTime
                    };
                    lastPIEvents.Enqueue(eventReceived);


                    if (AppSettings.WebMaxPIEvents > 0 && lastPIEvents.Count > AppSettings.WebMaxPIEvents)
                    {
                        var r = lastPIEvents.TryDequeue(out PIEventReceived piEventReceived);
                    }
                }
            }
            standbyManager.ReportPIConnectionSuccess();
        }

        public void PublishLastAFEvent(List<AFDataPipeEventWrapper> dpEvents)
        {
            if (!Enabled)
            {
                return;
            }
            foreach (var dpEvent in dpEvents)
            {
                if (dpEvent.Value.Attribute != null)
                {
                    AFEventReceived eventReceived = new AFEventReceived()
                    {
                        AttributePath = dpEvent.Value.Attribute.GetPath(),
                        Value = dpEvent.Value.Value,
                        Timestamp = dpEvent.Value.Timestamp.UtcTime
                    };
                    lastAFEvents.Enqueue(eventReceived);
                    if (AppSettings.WebMaxPIEvents > 0 && lastAFEvents.Count > AppSettings.WebMaxPIEvents)
                    {
                        var r = lastAFEvents.TryDequeue(out AFEventReceived piEventReceived);
                    }
                }
            }
            standbyManager.ReportPIConnectionSuccess();
        }

 

        public MonitoringInfo GetMonitoringInfo()
        {
            return new MonitoringInfo()
            {
                PIConnection = piConnectionInfo,
                EventsPerPoint = eventsPerPoint.Select(ev => new EventsPerPoint(ev.Key, ev.Value)).ToList(),
                EventsPerAttribute = eventsPerAttribute.Select(ev => new EventsPerAttribute(ev.Key, ev.Value)).ToList(),
                ResponsesPerCode = responsesPerCode.Select(ev => new ResponsesPerCode(ev.Key, ev.Value)).ToList(),
                TDEngineHttpResponses = tdEngineHttpResponses.ToList(),
                TDEngineInfo = new TDEngineInfo(AppSettings.TDEngineHost, AppSettings.TDEnginePort, AppSettings.tomlConfig.TDDataBase, tdEngineVersion),
                LastPIEvents = lastPIEvents.ToList(),
                LastAFEvents = lastAFEvents.ToList(),
            };
        }

        public List<ExceptionSummary> GetExceptions()
        {
            return this.piExceptions.Select(e => new ExceptionSummary(e)).ToList();
        }

        public void PublishPIConnectionStatus(PIConnection piConnectionInfo)
        {
            this.piConnectionInfo = piConnectionInfo;
            if (this.piConnectionInfo.IsConnected)
            {
                standbyManager.ReportPIConnectionSuccess();
            }
            else
            {
                standbyManager.ReportPIConnectionFailure();
            }
        }

        public void PublishTDEngineServerVersion(string tdEngineVersion)
        {
            this.tdEngineVersion = tdEngineVersion;
            standbyManager.ReportTDEngineConnectionSuccess();
        }

        public void PublishPIException(Exception e)
        {
            if (e is PIServerConnectionException)
            {
                standbyManager.ReportPIConnectionFailure();
            }
            this.piExceptions.Add(e);
        }

        public void PublishTDException(Exception e)
        {
            if (e is TDEngineTimeoutException)
            {
                standbyManager.ReportTDEngineConnectionFailure();
            }
            this.tdExceptions.Add(e);
           
        }
    }
}

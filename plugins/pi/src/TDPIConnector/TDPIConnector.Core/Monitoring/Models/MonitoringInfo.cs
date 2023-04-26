using System.Collections.Generic;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Monitoring.Models
{
    public class MonitoringInfo
    {
        public PIConnection PIConnection { get; internal set; }
        public List<EventsPerPoint> EventsPerPoint { get; internal set; }
        public List<EventsPerAttribute> EventsPerAttribute { get; internal set; }
        public List<TDEngineHttpResponseSummary> TDEngineHttpResponses { get; internal set; }
        public List<PIEventReceived> LastPIEvents { get; internal set; }
        public List<AFEventReceived> LastAFEvents { get; internal set; }
        public List<ResponsesPerCode> ResponsesPerCode { get; set; }
        public TDEngineInfo TDEngineInfo { get; internal set; }
   
    }
}

using System;

namespace TDPIConnector.Core.Monitoring
{
    public class PIEventReceived
    {
        public string Point { get; set; }
        public DateTime Timestamp { get; set; }
        public Object Value { get; set; }
    }
}

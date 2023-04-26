using System;

namespace TDPIConnector.Core.Monitoring
{
    public class AFEventReceived
    {
        public string AttributePath { get; set; }

        public DateTime Timestamp { get; set; }

        public Object Value { get; set; }
    }
}

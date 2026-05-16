using System;

namespace TDPIConnector.Core.Monitoring.Models
{
    public class ResponsesPerCode
    {
        public string HttpCode { get; set; }

        public string TDEngineCode { get; set; }

        public int Events { get; set; }

        public ResponsesPerCode(string key, int events)
        {
            this.HttpCode = key.Split('-')[0];
            this.TDEngineCode = key.Split('-')[1];
            this.Events = events;
        }
    }
}
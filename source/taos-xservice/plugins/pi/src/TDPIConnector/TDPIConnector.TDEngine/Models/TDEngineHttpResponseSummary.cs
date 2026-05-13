using System;

namespace TDPIConnector.TDEngine.Models
{
    public class TDEngineHttpResponseSummary
    {

        public TDEngineHttpResponseSummary(string url, int tdEngineCode, int httpStatusCode)
        {
            Url = url;
            TDEngineCode = tdEngineCode;
            HttpStatusCode = httpStatusCode;
        }

        public int HttpStatusCode { get; set; }
        public string Url { get; set; }
        public int TDEngineCode { get; set; }
        public DateTime Timestamp { get; set; }
    }
}

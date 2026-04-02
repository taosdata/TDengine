using System;

namespace TDPIConnector.TDEngine.Exceptions
{
    public class TDEngineHttpResponseException : Exception
    {
        private readonly Exception e;
        public TDEngineHttpResponseException(Exception e) : base("Error received when making HTTP requests against TD Engine", e)
        {
            this.e = e;            
        }

        public TDEngineHttpResponseException(int httpStatusCode, int tdEngineCode, string tdEngineMessage = null)
            : base($"Error received when making HTTP requests against TD Engine. HTTP Status Code: {httpStatusCode}, TDengine Code: {tdEngineCode}, TDengine Message: {tdEngineMessage}")
        {
            this.HttpStatusCode = httpStatusCode;
            this.TDEngineCode = tdEngineCode;
            this.TDEngineMessage = tdEngineMessage;
        }
        
        public int HttpStatusCode { get; }
        public int TDEngineCode { get; }
        public string TDEngineMessage { get; }
    }
}

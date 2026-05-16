using System;
using System.Runtime.Serialization;

namespace TDPIConnector.Core.Monitoring
{
    public class ExceptionSummary
    {
        public ExceptionSummary(Exception e)
        {
            Type = e.GetType().Name;      
            Message = e.Message;
            Stack = e.StackTrace;
            InnerExceptionType = (e.InnerException != null) ? e.InnerException.GetType().Name : "InnerException not available";
            InnerExceptionMessage = (e.InnerException != null) ? e.InnerException.Message : "InnerException not available";
            InnerExceptionStack = (e.InnerException != null) ? e.InnerException.StackTrace : "InnerException not available";
            Timestamp = DateTime.Now;
        }

        public string Type { get; set; }
        public string InnerExceptionType { get; }
        public string InnerExceptionMessage { get; }
        public string InnerExceptionStack { get; }
        public DateTime Timestamp { get; }
        public string Message { get; set; }
        public string Stack { get; set; }
    }
}
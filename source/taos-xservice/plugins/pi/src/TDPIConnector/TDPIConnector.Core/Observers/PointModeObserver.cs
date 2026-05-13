using log4net;
using System;
using TDPIConnector.PI;

namespace TDPIConnector.Core
{
    internal class PointModeObserver : IObserver<AFDataPipeEventWrapper>
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public event EventHandler<Exception> OnPIEventReceivedFailure = delegate { };
        private readonly EventsSender eventsSender;

        public PointModeObserver(EventsSender eventsSender)
        {
            this.eventsSender = eventsSender;
        }
        public void OnCompleted()
        {
            log.Info("Completed data pipe event observer");
        }

        public void OnError(Exception error)
        {      
            log.Error("Error receiving update from PI Data Archive", error);
            OnPIEventReceivedFailure(this, error);
        }

        public void OnNext(AFDataPipeEventWrapper dpEvent)
        {
            this.eventsSender.AddPIValue(dpEvent);
        }       
    }
}
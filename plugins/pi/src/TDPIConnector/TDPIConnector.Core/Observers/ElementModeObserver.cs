using log4net;
using System;
using TDPIConnector.PI;

namespace TDPIConnector.Core
{
    public class ElementModeObserver : IObserver<AFDataPipeEventWrapper>
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);  
        public event EventHandler<Exception> OnAFEventReceivedFailure = delegate { };
        private readonly EventsSender eventsSender;

        public ElementModeObserver(EventsSender eventsSender)
        {
            this.eventsSender = eventsSender;
        }
        public void OnCompleted()
        {
            log.Info("Completed data pipe event observer");
        }

        public void OnError(Exception error)
        {
            log.Error("Error receiving update from PI", error);
            OnAFEventReceivedFailure(this, error);
        }

        public void OnNext(AFDataPipeEventWrapper dpEvent)
        {
            this.eventsSender.AddAFValue(dpEvent);
        }
    }
}

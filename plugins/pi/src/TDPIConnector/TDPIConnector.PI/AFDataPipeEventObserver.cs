using OSIsoft.AF.Data;
using System;

namespace TDPIConnector.PI
{
    public class AFDataPipeEventObserver : IObserver<AFDataPipeEvent>
    {
        private readonly IObserver<AFDataPipeEventWrapper> observerWrapper;

        public AFDataPipeEventObserver(IObserver<AFDataPipeEventWrapper> observerWrapper)
        {
            this.observerWrapper = observerWrapper;
        }

        public void OnCompleted()
        {
            this.observerWrapper.OnCompleted();
        }

        public void OnError(Exception error)
        {
            this.observerWrapper.OnError(error);
        }

        public void OnNext(AFDataPipeEvent value)
        {
            this.observerWrapper.OnNext(new AFDataPipeEventWrapper(value));
        }
    }
}
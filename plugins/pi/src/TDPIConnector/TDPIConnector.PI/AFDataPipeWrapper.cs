using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System;
using System.Collections.Generic;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI
{
    public class AFDataPipeWrapper : IDisposable
    {
        private AFDataPipe afDataPipe;

        public AFDataPipeWrapper()
        {
            this.afDataPipe = new AFDataPipe();
        }

        public void GetObserverEvents()
        {
            AFErrors<AFAttribute> errors = this.afDataPipe.GetObserverEvents();
            if (errors != null && errors.HasErrors)
            {
                foreach (var errorKeyValuePair in errors.Errors)
                {
                    throw errorKeyValuePair.Value;
                }

                foreach (var errorKeyValuePair in errors.PIServerErrors)
                {
                    Exception exception = errorKeyValuePair.Value;
                    if (exception.Message == "[-10734] PINET: Broken Connection." || exception.Message == "[-10723] PINET: No Connection.")
                    {
                        throw new PIServerConnectionException(exception);
                    }
                    else
                    {
                        throw exception;
                    }
                }
            }
        }

        internal void Subscribe(IObserver<AFDataPipeEvent> observer)
        {
            this.afDataPipe.Subscribe(observer);
        }

        internal void AddSignups(List<AFAttribute> attributes)
        {
            AFErrors<AFAttribute> errors = this.afDataPipe.AddSignups(attributes);          
            if (errors != null && errors.HasErrors)
            {
                foreach (var errorKeyValuePair in errors.Errors)
                {
                     throw errorKeyValuePair.Value;
                }
            }
        }

        public void Dispose()
        {
            this.afDataPipe.Dispose();
        }
    }
}
using OSIsoft.AF;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI
{
    internal class PIDataPipeWrapper : IDisposable
    {
        private PIDataPipe piDataPipe;

        internal PIDataPipeWrapper()
        {
            this.piDataPipe = new PIDataPipe(AFDataPipeType.Snapshot);
        }

        internal void GetObserverEvents(int maxEventCountPerServer, out bool hasMoreEvents)
        {
            AFErrors<PIPoint> errors = this.piDataPipe.GetObserverEvents(maxEventCountPerServer, out hasMoreEvents);
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
            this.piDataPipe.Subscribe(observer);
        }

        internal void AddSignups(List<PIPoint> piPoints)
        {
            AFErrors<PIPoint> errors = this.piDataPipe.AddSignups(piPoints);
        
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
            this.piDataPipe.Dispose();
        }
    }
}
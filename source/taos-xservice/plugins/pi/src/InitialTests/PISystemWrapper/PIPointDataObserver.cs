using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PISystemWrapper
{
    public delegate void ProcessAFDataPipeEventDelegate(AFDataPipeEvent evt);
    public delegate void AFDataPipeNoMoreEventsDelegate(int eventsProccessed, DateTime timestamp);

    class PIPointDataObserver : IObserver<AFDataPipeEvent>, IDisposable
    {
        // The list of attributes to monitor
        public IList<PIPoint> PointList { get; set; }

        // The underlying AFDataPipe that provides incoming values
        public PIDataPipe DataPipe { get; set; }

        // Interval to wait in between calling the data pipe
        private int _threadSleepTimeInMilliseconds;

        // The client provides this delegate to call during OnNext()
        private ProcessAFDataPipeEventDelegate _processEvent;

        private AFDataPipeNoMoreEventsDelegate _noMoreEvents;

        private int eventCount = 0;

        public PIPointDataObserver(IList<PIPoint> points, ProcessAFDataPipeEventDelegate processEvent)
        {
            _processEvent = processEvent;
            PointList = points;

            //can sign up for snapshot, archive, or both (Timeseries)
            DataPipe = new PIDataPipe(AFDataPipeType.Snapshot);

            _threadSleepTimeInMilliseconds = 500;

            Console.WriteLine("Subscribed");

        }

        public AFErrors<PIPoint> Start()
        {
            // Subscribe this object (Observer) to the AFDataPipe (Observable)
            DataPipe.Subscribe(this);

            // The data pipe will provide updates from attributes inside AttributeList
            AFErrors<PIPoint> errors = DataPipe.AddSignups(PointList);

            if (errors != null)
            {
                return errors;
            }
            else
            {
                // This task loop calls GetObserverEvents every x seconds
                Task mainTask = Task.Run(() =>
                {
                    bool hasMoreEvents = false;
                    eventCount = 0;
                    while (true)
                    {
                            //Console.WriteLine($"checkng events");

                        AFErrors<PIPoint> results = DataPipe.GetObserverEvents(1000, out hasMoreEvents);
                        //Console.WriteLine($"check done");

                        if (!hasMoreEvents)
                    {
                            //Console.WriteLine($"No Events, waiting {_threadSleepTimeInMilliseconds/1000} seconds");
                            //_noMoreEvents(eventCount, DateTime.Now);
                            eventCount = 0;
                            Thread.Sleep(_threadSleepTimeInMilliseconds);
                        }
                    }
                });
                return null;
            }
        }

        public void OnNext(AFDataPipeEvent dpEvent)
        {
            // AFDataPipeEvent contains the AFValue representing the incoming event
            eventCount++;
            var afValue = dpEvent.Value;
            var point = dpEvent.Value.PIPoint;
            //Console.WriteLine($"On Next: {point.Name} - {afValue.Value.ToString()} {afValue.Timestamp.ToString()}");

            _processEvent(dpEvent);
        }

        public void Dispose()
        {
            DataPipe.Dispose();
            DataPipe = null;
        }


        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}

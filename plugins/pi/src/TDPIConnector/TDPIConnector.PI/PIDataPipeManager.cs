using log4net;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;

namespace TDPIConnector.PI
{
    public class PIDataPipeManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly List<PIDataPipeWrapper> piDataPipes;
        private readonly List<List<PIPoint>> pointSetLists;

        public PIDataPipeManager(int numberOfDataPipes)
        {
            piDataPipes = new List<PIDataPipeWrapper>(numberOfDataPipes);
            pointSetLists = new List<List<PIPoint>>(piDataPipes.Count);
            for (int i = 0; i < numberOfDataPipes; i++)
            {
                piDataPipes.Add(new PIDataPipeWrapper());
                pointSetLists.Add(new List<PIPoint>());
            }
        }

        public void Subscribe(IObserver<AFDataPipeEventWrapper> observerWrapper)
        {
            IObserver<AFDataPipeEvent> observer = new AFDataPipeEventObserver(observerWrapper);
            for (int i = 0; i < piDataPipes.Count; i++)
            {
                piDataPipes[i].Subscribe(observer);
            }
        }

        public void AddSignups(IList<PIPoint> piPoints)
        {

            for (int i = 0; i < piPoints.Count; i++)
            {
                int k = i % piDataPipes.Count;
                pointSetLists[k].Add(piPoints[i]);
            }

            for (int i = 0; i < piDataPipes.Count; i++)
            {
                piDataPipes[i].AddSignups(pointSetLists[i]);
            }
        }

        public void GetObserverEvents(int maxEventCount)
        {
            for (int i = 0; i < piDataPipes.Count; i++)
            {
#pragma warning disable IDE0059 // Unnecessary assignment of a value
                piDataPipes[i].GetObserverEvents(maxEventCount, out bool hasMoreEvents);
#pragma warning restore IDE0059 // Unnecessary assignment of a value
            }
        }

        public void Dispose()
        {
            for (int i = 0; i < piDataPipes.Count; i++)
            {
                log.Info($"Disposing PI Data Pipe {i}.");
                piDataPipes[i].Dispose();
            }
        }
    }
}
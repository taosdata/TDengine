using log4net;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System;
using System.Collections.Generic;

namespace TDPIConnector.PI
{
    public class AFDataPipeManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly List<AFDataPipeWrapper> afDataPipes;
        private readonly List<List<AFAttribute>> attributeSetLists;

        public AFDataPipeManager(int numberOfDataPipes)
        {
            afDataPipes = new List<AFDataPipeWrapper>(numberOfDataPipes);
            attributeSetLists = new List<List<AFAttribute>>(afDataPipes.Count);
            for (int i = 0; i < numberOfDataPipes; i++)
            {
                afDataPipes.Add(new AFDataPipeWrapper());
                attributeSetLists.Add(new List<AFAttribute>());
            }         
        }

        public void Subscribe(IObserver<AFDataPipeEventWrapper> observerWrapper)
        {
            IObserver<AFDataPipeEvent> observer = new AFDataPipeEventObserver(observerWrapper);
            for (int i = 0; i < afDataPipes.Count; i++)
            {
                
                afDataPipes[i].Subscribe(observer);
            }
        }
        public void AddSignups(List<AFElementWrapper> elements)
        {
            for (int i = 0; i < elements.Count; i++)
            {
                int k = i % afDataPipes.Count;
                if (elements[i].HasInvalidAttr()) continue;
                attributeSetLists[k].AddRange(elements[i].AFSDKObject.Attributes);
            }

            for (int i = 0; i < afDataPipes.Count; i++)
            {
                afDataPipes[i].AddSignups(attributeSetLists[i]);     
            }
        }

        public void GetObserverEvents()
        {
            for (int i = 0; i < afDataPipes.Count; i++)
            {
                afDataPipes[i].GetObserverEvents();
            }
        }

        public void Dispose()
        {
            for (int i = 0; i < afDataPipes.Count; i++)
            {
                log.Info($"Disposing AF Data Pipe {i}.");
                afDataPipes[i].Dispose();        
            }
        }
    }
}
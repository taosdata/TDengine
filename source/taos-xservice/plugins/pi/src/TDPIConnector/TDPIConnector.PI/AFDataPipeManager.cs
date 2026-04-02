using log4net;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TDPIConnector.PI
{
    public class AFDataPipeManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly List<AFDataPipeWrapper> afDataPipes;
        private readonly List<List<AFAttribute>> attributeSetLists;
        private readonly Dictionary<string, int> templaltePipeDic = new Dictionary<string, int>();

        const int MaxPipeSignCounts = 10000;

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
            List<Task> tasks = new List<Task>();

            for (int i = 0; i < afDataPipes.Count; i++)
            {
                int batchNum = i;
                tasks.Add(Task.Run(async () =>
                {
                    int thisBatchSize = 0;
                    for (int num = batchNum; num < elements.Count; num += afDataPipes.Count)
                    {
                        foreach (AFAttributeWrapper attr in elements[num].Attributes)
                        {
                            if (attr.Valid() && attr.signUpValid() && !attr.Unsupported())
                            {
                                attributeSetLists[batchNum].Add(attr.AFSDKObject);
                            }
                        }
                        thisBatchSize++;
                        if (thisBatchSize % 100 == 0)
                        {
                            log.Info($"AddSignups check thread:{batchNum} {thisBatchSize} element finished.");
                            afDataPipes[batchNum].AddSignups(attributeSetLists[batchNum]);
                            attributeSetLists[batchNum].Clear();
                        }
                    }
                    afDataPipes[batchNum].AddSignups(attributeSetLists[batchNum]);
                    attributeSetLists[batchNum].Clear();
                    log.Info($"AddSignups check thread:{batchNum} all element({thisBatchSize}) finished.");
                }));
            }
            Task.WaitAll(tasks.ToArray());
        }

        private int GetLowPressurePipe() {
            int minVal = int.MaxValue;
            int index = 0;
            for(int i = 0; i < afDataPipes.Count; ++i) {
                if (0 == afDataPipes[i].SignupAttrCount)
                {
                    return i;
                }
                else if(afDataPipes[i].SignupAttrCount < minVal) {
                    minVal = afDataPipes[i].SignupAttrCount;
                    index = i;
                }
            }
            return index;
        }

        private int GetDataPipeIndexByTemplate(ref string templateName) {
            if (templaltePipeDic.ContainsKey(templateName))
            {
                if (afDataPipes[templaltePipeDic[templateName]].SignupAttrCount > MaxPipeSignCounts)
                {
                    int index = GetLowPressurePipe();
                    templaltePipeDic[templateName] = index;
                }
                return templaltePipeDic[templateName];
            }
            else {
                int index = GetLowPressurePipe();
                templaltePipeDic[templateName] = index;
                return index;
            }
        }
        public void AddSignupAttributes(ref string templateName, ref List<AFAttribute> attributes)
        {
            var index = GetDataPipeIndexByTemplate(ref templateName);
            afDataPipes[index].AddSignups(attributes);
            afDataPipes[index].SignupAttrCount += attributes.Count;
        }

        public void RetrySignUpBatchAttributes(ref string templateName, ref List<AFAttribute> attributes)
        {
            var index = GetDataPipeIndexByTemplate(ref templateName);
            foreach (var attr in attributes) {
                try
                {
                    afDataPipes[index].AddSignups(new List<AFAttribute> { attr });
                    afDataPipes[index].SignupAttrCount += 1;
                }
                catch (Exception e){
                    log.Error($"SignUp retry failed! {e.Message}!");
                }
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
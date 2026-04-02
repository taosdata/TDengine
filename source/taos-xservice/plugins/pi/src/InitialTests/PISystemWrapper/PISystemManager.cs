using OSIsoft.AF;
using OSIsoft.AF.Analysis;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Search;
using OSIsoft.AF.Time;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PISystemWrapper
{
    public delegate void DataPipeEventDelegate(AFDataPipeEventWrapper evt);

    public class PISystemManager
    {
        private PIServer piServer;
        private PISystem piSystem;

        public PISystemManager()
        {
            this.piServer = new PIServers()["MARC-PI2018"];
            this.piSystem = new PISystems()["MARC-PI2018"];
        }

        public PISystemManager(string piServer, string piSystem)
        {
            this.piServer = new PIServers()[piServer];
            this.piSystem = new PISystems()[piSystem];
        }

        public void Connect()
        {
            this.piServer.Connect();
            this.piSystem.Connect();
        }

        public IEnumerable<AFElementTemplateWrapper> GetElementTemplates(string afDatabaseName)
        {
            AFDatabase afDatabase = piSystem.Databases[afDatabaseName];
            return afDatabase
                .ElementTemplates
                .Where(et => et.InstanceType == typeof(AFElement))
                .Select(e => new AFElementTemplateWrapper(e));
        }

        public IEnumerable<AFElementWrapper> GetElementTemplateInstances(AFElementTemplateWrapper elementTemplate)
        {

            using (var search = new AFElementSearch(elementTemplate.AFSDKObject.Database, "Find_" + elementTemplate.Name, $"Template: '{elementTemplate.Name}'"))
            {
                IEnumerable<AFElement> elements = search.FindObjects(fullLoad: true);
                return elements.Select(e => new AFElementWrapper(e));
            }

        }

        public PIPointWrapper FindPIPoint(string tag)
        {
            PIPoint piPoint = PIPoint.FindPIPoint(piServer, tag);
            string pointType = piPoint.GetAttribute("pointtype").ToString();
            int pointId = Convert.ToInt32(piPoint.GetAttribute("pointid"));
            return new PIPointWrapper(piPoint, pointType, pointId);
        }

        public async Task<List<AFValueWrapper>> GetPIPointRecordedValuesByCount(PIPointWrapper piPoint, DateTime startTime, int count, bool forward)
        {
            DateTime currentDateTime = startTime;
            List<AFValueWrapper> valuesWrapper = new List<AFValueWrapper>();
            do
            {
                AFValues values = await piPoint.AFSDKObject.RecordedValuesByCountAsync(
                    new AFTime(currentDateTime),
                    count,
                    forward,
                    OSIsoft.AF.Data.AFBoundaryType.Inside,
                    string.Empty,
                    true);



                foreach (AFValue val in values)
                {
                    valuesWrapper.Add(new AFValueWrapper(val));
                }


                if (values.Count == 0)
                {
                    currentDateTime = DateTime.MaxValue;
                }
                else
                {
                    currentDateTime = values.ToArray().ToList().Last().Timestamp.LocalTime.AddMilliseconds(1);
                }
            } while (currentDateTime != DateTime.MaxValue);
            return valuesWrapper;
        }

        public Dictionary<string, AFValuesWrapper> GetAFAttributesInterpolatesValues(AFAttributesWrapper attributes)
        {
            AFAttributeList attributeList = new AFAttributeList(attributes.AFSDKObject);
            IEnumerable<AFValues> minDateResult = attributeList.Data.RecordedValuesByCount(
                new AFTime(DateTime.MinValue),
                1,
                true,
                OSIsoft.AF.Data.AFBoundaryType.Inside,
                string.Empty,
                false,
                new PIPagingConfiguration(PIPageType.EventCount, 1000));

            DateTime minDateTime = minDateResult.Select(r => r.First().Timestamp.LocalTime).Min();

            IEnumerable<AFValues> valuesList = attributeList.Data.InterpolatedValues(
                new AFTimeRange(new AFTime(minDateTime), AFTime.Now),
                new AFTimeSpan(0, 0, 0, 0, 1),
                string.Empty,
                true,
                new PIPagingConfiguration(PIPageType.EventCount, 1000));

  
            Dictionary<string, AFValuesWrapper> dic = new Dictionary<string, AFValuesWrapper>();

            int i = 0;
            foreach (var values in valuesList)
            {
                dic.Add(attributes[i].Name, new AFValuesWrapper(values));
                i++;
            }

            return dic;
        }

        private DataPipeEventDelegate _eventDelegate;

        public void Subscribe(List<string> points, DataPipeEventDelegate eventDelegate)
        {
            _eventDelegate = eventDelegate;
            // List to hold the PIPoint objects that we will sign up for updates on
            List<string> ptattList = new List<string>();
            // Add points we are interested in
            ptattList.Add(PICommonPointAttributes.PointType);
            ptattList.Add(PICommonPointAttributes.Zero);
            ptattList.Add(PICommonPointAttributes.Span);

            IList<PIPoint> piPointList = PIPoint.FindPIPoints(piServer, points, ptattList);

            //register for events
            var dataSubscription = new PIPointDataObserver(piPointList, new ProcessAFDataPipeEventDelegate(EventProcessing));
            //eventBuffer = new List<AFValue>();
            dataSubscription.Start();

            Console.ReadKey();

        }

        private void EventProcessing(AFDataPipeEvent pipeEvent)
        {
            _eventDelegate(new AFDataPipeEventWrapper(pipeEvent));
        }
    }
}

using log4net;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Search;
using OSIsoft.AF.Time;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI
{
    public class DateTimeWrapper
    {
        public DateTime Value { get; set; }
    }
    public class PISystemManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly PISystem piSystem;

        public PISystemManager(string piSystem)
        {
            this.piSystem = new PISystems()[piSystem];
        }


        public void Connect()
        {
            if (piSystem == null)
            {
                throw new Exception("AF Server not found.");
            }

            try
            {
                piSystem.Connect();
                log.Info($"AF Server Connected = {piSystem.ConnectionInfo.IsConnected}");

            }
            catch (Exception e)
            {
                log.Error($"Error connecting to AF Server.", e);
                PISystemConnectionException piSystemConnectionException = new PISystemConnectionException(e);
                throw piSystemConnectionException;
            }
        }
        
        public List<PIPointWrapper> GetPIPointsFromElementTemplates(string afDatabaseName, List<string> elementTemplates)
        {
            
            //get all elements based on template and points
            List<AFElementWrapper> elements = new List<AFElementWrapper>();
            foreach (var elementTemplate in elementTemplates)
            {
                var element = GetElementsByTemplate(afDatabaseName, elementTemplate).ToList();
                elements.AddRange(element);
            }

            //check for points table on TD Engine and create if needed
            var piPoints = new Dictionary<int, PIPointWrapper>();
            foreach (var element in elements)
            {
                foreach (var attribute in element.Attributes)
                {
                    if (attribute.IsPIPointDataReference)
                    {
                        if (!piPoints.ContainsKey(attribute.PIPoint.ID))
                            piPoints.Add(attribute.PIPoint.ID, attribute.PIPoint);
                    }
                }
            }

            return piPoints.Values.ToList();

        }

        public AFDataPipeManager AddSignups(List<AFElementWrapper> elements, IObserver<AFDataPipeEventWrapper> observerWrapper, int numberOfDataPipes)
        {
            AFDataPipeManager afDataPipeManager = new AFDataPipeManager(numberOfDataPipes);
            afDataPipeManager.Subscribe(observerWrapper);
            afDataPipeManager.AddSignups(elements);
            return afDataPipeManager;
        }

        public IEnumerable<AFElementTemplateWrapper> GetElementTemplates(string afDatabaseName, List<string> templates)
        {
            AFDatabase afDatabase = piSystem.Databases[afDatabaseName];
            if (afDatabase == null)
            {
                throw new Exception("AF Database not found.");
            }
            return afDatabase
                .ElementTemplates
                .Where(et => et.InstanceType == typeof(AFElement) && templates.Contains(et.Name))
                .Select(e => new AFElementTemplateWrapper(e));
        }
        public IEnumerable<AFElementTemplateWrapper> GetElementTemplates(string afDatabaseName)
        {
            AFDatabase afDatabase = piSystem.Databases[afDatabaseName];
            if (afDatabase == null)
            {
                throw new Exception("AF Database not found.");
            }
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

        public IEnumerable<AFElementWrapper> GetElementsByTemplate(string afDatabaseName, string elementTemplateName)
        {
            AFDatabase afDatabase = piSystem.Databases[afDatabaseName];
            if (afDatabase == null)
            {
                throw new Exception($"Could not find AF Database {afDatabaseName}.");
            }
            AFElementTemplate elementTemplate = afDatabase.ElementTemplates[elementTemplateName];
            if (elementTemplate == null)
            {
                throw new Exception($"Could not find AF Element Template {elementTemplateName}.");
            }
            using (var search = new AFElementSearch(afDatabase, "TemplateSearch", $"TemplateName: '{elementTemplateName}'"))
            {
                IEnumerable<AFElement> elements = search.FindObjects(fullLoad: true);
                return elements.Select(e => new AFElementWrapper(e));
            }
        }

        public static async Task<List<AFValueWrapper>> GetPIPointRecordedValuesByCountForward(PIPointWrapper piPoint, DateTimeWrapper startTime, int count)
        {
            List<AFValueWrapper> valuesWrapper = new List<AFValueWrapper>();
            AFValues values = await piPoint.AFSDKObject.RecordedValuesByCountAsync(
                new AFTime(startTime.Value),
                count,
                true,
                OSIsoft.AF.Data.AFBoundaryType.Inside,
                string.Empty,
                true);

            foreach (AFValue val in values)
            {
                valuesWrapper.Add(new AFValueWrapper(val));
            }

            if (values.Count == 0)
            {
            startTime.Value = DateTime.MaxValue;
            }
            else
            {
                startTime.Value = values.ToArray().ToList().Last().Timestamp.LocalTime.AddMilliseconds(1);
            }
            return valuesWrapper;
        }

        public static List<AFValueWrapper> GetPIPointRecordedValues(PIPointWrapper piPoint, ref DateTime startTime, DateTime endTime, int count)
        {
            List<AFValueWrapper> valuesWrapper = new List<AFValueWrapper>();
                AFValues values = piPoint.AFSDKObject.RecordedValues(
                    new AFTimeRange(startTime, endTime),
                    OSIsoft.AF.Data.AFBoundaryType.Inside,
                    string.Empty,
                    true, count);

            foreach (AFValue val in values)
            {
                valuesWrapper.Add(new AFValueWrapper(val));
                // skip invalid timestamp
                if (val.Timestamp != AFTime.MaxValue)
                {
                    startTime = startTime > val.Timestamp.LocalTime ? startTime : val.Timestamp.LocalTime;
                }
            }

            if (values.Count == 0)
            {
                startTime = DateTime.MaxValue;
            }
            else if (startTime != DateTime.MaxValue)
            {
                startTime = startTime.AddMilliseconds(1);
            }
            return valuesWrapper;
        }

        public static async Task<List<AFValueWrapper>> GetPIPointRecordedValuesByCountReverse(PIPointWrapper piPoint, DateTimeWrapper startTime, int count)
        {
            List<AFValueWrapper> valuesWrapper = new List<AFValueWrapper>();
            AFValues values = await piPoint.AFSDKObject.RecordedValuesByCountAsync(
                new AFTime(startTime.Value),
                count,
                false,
                AFBoundaryType.Inside,
                string.Empty,
                true);

            foreach (AFValue val in values)
            {
                valuesWrapper.Add(new AFValueWrapper(val));
            }

            if (values.Count == 0)
            {
                startTime.Value = DateTime.MinValue;
            }
            else
            {
                startTime.Value = values.ToArray().ToList().Last().Timestamp.LocalTime.AddMilliseconds(-1);
            }
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

        public IEnumerable<AFValuesWrapper> GetAttributesRecordedValues(AFAttributeListWrapper afAttributes, DateTime startTime, DateTime endTime, int count)
        {
            DateTime currentDateTime = startTime;
            AFAttributeList attributeList = afAttributes.AFSDKObject;
           
            PIPagingConfiguration config = new PIPagingConfiguration(PIPageType.TagCount, count);

            try
            {
                IEnumerable<AFValues> afValuesList = attributeList.Data.RecordedValues(new AFTimeRange(currentDateTime, endTime), AFBoundaryType.Inside, null, false, config);
                return afValuesList.Select(v => new AFValuesWrapper(v));
            }
            catch (OperationCanceledException)
            {
                // Errors that occur during bulk calls get trapped here
                // The actual error is stored on the PIPagingConfiguration object
                throw(config.Error);
            }
            catch (Exception ex)
            {
                // Errors that occur in an iterative fallback method get trapped here
                throw ex;
            }
        }

        public void Dispose()
        {
            this.piSystem.Dispose();
        }
    }
}

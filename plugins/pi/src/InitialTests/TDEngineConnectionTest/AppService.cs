using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDEngineHttpClient.Models;
using TDEngineHttpClient;
using System.Diagnostics;
using PISystemWrapper;
using TDEngineHttpClient.Helper;

namespace TDEngineConnectionTest
{
    internal class AppService
    {
        private TDEngineClient tdEngineClient;
        private PISystemManager piSystemManager;
        private List<string> piPoints;

        public AppService()
        {
            //this.tdEngineClient = new TDEngineClient("http://gw-aws.cloud.tdengine.com:80", "9f336b1c60931a3af97ba3e51e9f56b3467da3ff");
            this.tdEngineClient = new TDEngineClient("10.10.0.34", 6041, "root", "taosdata");

            //this.tdEngineClient = new TDEngineClient("172.19.96.1", 6041, "root", "taosdata");

            this.piSystemManager = new PISystemManager();

        }

        public AppService(string piServer, string piSystem, string tdHostname, int tdPort, string tdUsername, string tdPassword, List<string> piPoints)
        {
            this.tdEngineClient = new TDEngineClient(tdHostname, tdPort, tdUsername, tdPassword);
            this.piSystemManager = new PISystemManager(piServer, piSystem);
            this.piPoints = piPoints;
        }

        internal async Task Connect()
        {
            this.piSystemManager.Connect();
            string resp = await tdEngineClient.GetServerVersion();
            Console.WriteLine(resp);
        }

        internal async Task MigrateAFDatabase(string afDatabaseName)
        {
            string dbName = "pi";
            var resp1 = await this.tdEngineClient.CreateDatabase(dbName);


            IEnumerable<AFElementTemplateWrapper> elementTemplates = piSystemManager.GetElementTemplates(afDatabaseName);
            foreach (AFElementTemplateWrapper elementTemplate in elementTemplates)
            {
                string superTableName = await this.CreateSuperTable(elementTemplate);
                IEnumerable<AFElementWrapper> elements = piSystemManager.GetElementTemplateInstances(elementTemplate);
                foreach (AFElementWrapper element in elements)
                {
                    List<string> uoms = UomConverter.Convert(element.Attributes);
                    string resp = await this.tdEngineClient.CreateTableForAFElement(dbName, superTableName, element.Name, element.ID, uoms);
                    await MigrateAFElementData(element);
                }
            }
            await Task.Delay(1000);
        }



        private async Task<string> CreateSuperTable(AFElementTemplateWrapper elementTemplate)
        {
            IEnumerable<TDColumn> columns = AttributeColumnConverter.Convert(elementTemplate.AttributeTemplates);
            string superTableName = elementTemplate.Name.ToTDEngineNamingPattern();
            string resp = await this.tdEngineClient.CreateSuperTableForAFElement("pi", superTableName, columns);
            return superTableName;
        }

        internal async Task MigratePIPoints(List<string> tags)
        {
            foreach (string tag in tags)
            {
                Console.WriteLine("Processing PI Point: " + tag);
                await MigratePIPoint(tag);
            }
        }

        internal async Task MigratePIPoint(string tag)
        {
            PIPointWrapper piPoint = this.piSystemManager.FindPIPoint(tag);
            string tdColumnType = PointTypeConverter.Convert(piPoint.PointType);

            string dbName = "pi";
            string superTableName = $"pitag_{tdColumnType.ToLower()}";
            if (tdColumnType.ToLower() == "nchar")
            {
                tdColumnType += "(100)";
            }
            var resp1 = await this.tdEngineClient.CreateDatabase(dbName);
            string resp2 = await this.tdEngineClient.CreateSuperTableForPIPoint(dbName, superTableName, tdColumnType);
            string resp3 = await this.tdEngineClient.CreateTableForPIPoint(dbName, superTableName, piPoint.Name, piPoint.PointId);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            DateTime currentDateTime = DateTime.MinValue;
            List<AFValueWrapper> afValues = await piSystemManager.GetPIPointRecordedValuesByCount(piPoint, currentDateTime, 5000, true);

            Console.WriteLine("PI retrieval duration:" + stopwatch.ElapsedMilliseconds + "ms");
            stopwatch.Reset();
            stopwatch.Start();

            List<TDValue> tdValues = afValues
                .Select(afValue => afValue.ToTDValue())
                .ToList();

            await this.tdEngineClient.InsertValuesForPI(dbName, piPoint.Name, tdValues);
            stopwatch.Stop();
            Console.WriteLine("TDEngine duration:" + stopwatch.ElapsedMilliseconds + "ms");

        }


        private async Task MigrateAFElementData(AFElementWrapper element)
        {
            Console.WriteLine("Processing AF Element: " + element.Name);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AFAttributesWrapper attributes = element.Attributes;
            Dictionary<string, AFValuesWrapper> valuesDic = piSystemManager.GetAFAttributesInterpolatesValues(attributes);

            Console.WriteLine("PI retrieval duration:" + stopwatch.ElapsedMilliseconds + "ms");
            stopwatch.Reset();
            stopwatch.Start();

            int count = valuesDic.First().Value.Count;
            List<string> keys = valuesDic.Keys.ToList();
            keys.Sort();
            List<TDValues> tdValuesList = new List<TDValues>();
            for (int i = 0; i < count; i++)
            {
                List<TDValue> tdValues = new List<TDValue>();
                foreach (string key in keys)
                {
                    AFValueWrapper afValue = valuesDic[key][i];
                    TDValue value = afValue.ToTDValue();
                    tdValues.Add(value);
                }
                tdValuesList.Add(new TDValues(tdValues));
            }
            await this.tdEngineClient.InsertValuesForAF("pi", element.Name, tdValuesList);


            Console.WriteLine("TDEngine duration:" + stopwatch.ElapsedMilliseconds + "ms");
            stopwatch.Stop();
        }

        private Dictionary<string, AFValuesWrapper> ConvertToDic(IEnumerable<AFValuesWrapper> valuesList)
        {
            Dictionary<string, AFValuesWrapper> dic = new Dictionary<string, AFValuesWrapper>();
            foreach (AFValuesWrapper values in valuesList)
            {
                string attributeName = values[0].Attribute.Name;
                dic.Add(attributeName, values);
            }
            return dic;

        }

        internal void Subscribe()
        {
            piSystemManager.Connect();
            piSystemManager.Subscribe(piPoints, new DataPipeEventDelegate(DataPipeEvents));
        }

        internal void DataPipeEvents(AFDataPipeEventWrapper pipeEvent)
        {
            //Console.WriteLine($"from app service {pipeEvent.Value.ValueAsDouble()}");
            List<TDValue> currentTdValues = new List<TDValue>();
            currentTdValues.Add(pipeEvent.Value.ToTDValue());

            string tdColumnType = PointTypeConverter.Convert(pipeEvent.Point.PointType);

            string dbName = "pi";
            string superTableName = $"pitag_{tdColumnType.ToLower()}";
            if (tdColumnType.ToLower() == "nchar")
            {
                tdColumnType += "(100)";
            }

            this.tdEngineClient.InsertValuesForPIInSeries(dbName, pipeEvent.Point.Name, currentTdValues).Wait();
            Console.WriteLine($"Sent {pipeEvent.Point.Name} {pipeEvent.Value.Value.ToString()} {pipeEvent.Value.Timestamp.UtcTime.ToString()} to TDEngine");

        }

    }
}
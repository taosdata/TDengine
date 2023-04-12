using log4net;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;
using PISimulator.Core.Config;
using System;
using System.Collections.Generic;
using System.Net;

namespace PISimulator.Core
{
    public class PISystemManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PISystem piSystem;
        private PIServer piServer;
        private Random random;
        private static Dictionary<Type, string> pointTypeConverter = new Dictionary<Type, string>()
        {
            [typeof(int)] = "Int32",
            [typeof(double)] = "Float32",
            [typeof(string)] = "String",
            [typeof(DateTime)] = "Timestamp",
        };

        public PISystemManager()
        {
            StartTime = DateTime.Now;
        }


        internal void SendValues(AFSettingsConfig afSettingsConfig)
        {
            AFDatabase db = piSystem.Databases[afSettingsConfig.AFDatabase];

            IList<AFValue> values = new List<AFValue>();
            AFTime afTime = new AFTime(DateTime.Now);
            foreach (var stateKeyPair in afSettingsConfig.AFTree)
            {
                string stateName = stateKeyPair.Key;
                StateConfig stateConfig = stateKeyPair.Value;
                AFElement stateElement = db.Elements[stateName];

                foreach (var cityKeyPair in stateConfig)
                {
                    string cityName = cityKeyPair.Key;
                    CityConfig cityConfig = cityKeyPair.Value;
                    AFElement cityElement = stateElement.Elements[cityName];

                    foreach (MeterConfig meterConfig in cityConfig.Meters)
                    {
                        string meterName = GetMeterElement(meterConfig.Id);
                        AFElement meterElement = cityElement.Elements[meterName];
                        AFAttribute currentAttribute = meterElement.Attributes["Current"];
                        AFAttribute voltageAttribute = meterElement.Attributes["Voltage"];
                        MeterAttributeConfig currentConfig = meterConfig.Current;
                        MeterAttributeConfig voltageConfig = meterConfig.Voltage;

                        AFValue afCurrentValue = new AFValue(currentAttribute, GenerateValue(currentConfig, meterConfig.TimePeriod, meterConfig.Type), afTime);
                        AFValue afVoltageValue = new AFValue(voltageAttribute, GenerateValue(voltageConfig, meterConfig.TimePeriod, meterConfig.Type), afTime);
                        values.Add(afCurrentValue);
                        values.Add(afVoltageValue);
                    }
                }
            }

            var errors = AFListData.UpdateValues(values, AFUpdateOption.Replace);
            if (errors != null && errors.HasErrors)
            {
                foreach (var error in errors.Errors)
                {
                    log.Error(error.Key + ":" + error.Value);

                }
                foreach (var error in errors.PIServerErrors)
                {
                    log.Error(error.Key + ":" + error.Value);

                }

                foreach (var error in errors.PISystemErrors)
                {
                    log.Error(error.Key + ":" + error.Value);

                }
            }

        }

        private double GenerateValue(MeterAttributeConfig attributeConfig, int timePeriod, DataTypeEnum dataTypeEnum)
        {
            double noise = attributeConfig.RandomNoise * random.NextDouble();
            switch (dataTypeEnum)
            {
                case DataTypeEnum.SinusoidWave:
                    {
                        return GenerateSinusoidValue(attributeConfig, timePeriod) + noise;
                    }
                case DataTypeEnum.SquareWave:
                    {
                        return GenerateSquaredValue(attributeConfig, timePeriod) + noise;
                    }
                case DataTypeEnum.TriangleWave:
                    {
                        return GenerateTriangleValue(attributeConfig, timePeriod) + noise;
                    }
                default:
                    {
                        return GenerateSinusoidValue(attributeConfig, timePeriod) + noise;
                    }
            }
        }

        private double GenerateTriangleValue(MeterAttributeConfig attributeConfig, int frequencySeconds)
        {
            DateTime now = DateTime.Now;
            double totalSecondsOfDay = (now - now.Date).TotalSeconds;
            double cycles = totalSecondsOfDay / frequencySeconds;
            cycles = cycles - Math.Floor(cycles);
            double range = attributeConfig.HigherLimit - attributeConfig.LowerLimit;
            double cyclesOffset = attributeConfig.OffsetPhase / 360;
            double triangleValue = GetTriangleValue(cycles + cyclesOffset);
            return attributeConfig.LowerLimit + (0.5 * range * (1 + triangleValue));
        }

        private double GetTriangleValue(double cycles)
        {
            double rest = cycles - Math.Floor(cycles);
            if (rest >= 0 && rest <= 0.25)
            {
                return rest * 4;

            }
            else if (rest >= 0.25 && rest <= 0.75)
            {
                return 1 - (rest - 0.25) * 4;
            }
            else if (rest >= 0.75 && rest <= 1)
            {
                return -1 + (rest - 0.75) * 4;
            }
            throw new Exception();
        }

        private double GenerateSquaredValue(MeterAttributeConfig attributeConfig, int frequencySeconds)
        {
            DateTime now = DateTime.Now;
            double totalSecondsOfDay = (now - now.Date).TotalSeconds;
            double cycles = totalSecondsOfDay / frequencySeconds;
            cycles = cycles - Math.Floor(cycles);
            double range = attributeConfig.HigherLimit - attributeConfig.LowerLimit;
            double cyclesOffset = attributeConfig.OffsetPhase / 360;
            double sinusoidValue = Math.Sin((cycles + cyclesOffset) * 2 * Math.PI);
            double squaredValue = sinusoidValue >= 0 ? 1 : -1;
            return attributeConfig.LowerLimit + (0.5 * range * squaredValue);
        }

        private double GenerateSinusoidValue(MeterAttributeConfig attributeConfig, int frequencySeconds)
        {
            DateTime now = DateTime.Now;
            double totalSecondsOfDay = (now - now.Date).TotalSeconds;
            double cycles = totalSecondsOfDay / frequencySeconds;
            cycles = cycles - Math.Floor(cycles);
            double range = attributeConfig.HigherLimit - attributeConfig.LowerLimit;
            double cyclesOffset = attributeConfig.OffsetPhase / 360;
            return attributeConfig.LowerLimit + (0.5 * range * (Math.Sin((cycles + cyclesOffset) * 2 * Math.PI)));
        }

        private static Dictionary<string, Type> typeConverter = new Dictionary<string, Type>()
        {
            ["int"] = typeof(int),
            ["double"] = typeof(double),
            ["string"] = typeof(string),
            ["DateTime"] = typeof(DateTime),
        };

        public DateTime StartTime { get; }

        public PISystemManager(string piSystemName)
        {
            this.piSystem = new PISystems()[piSystemName];
            this.piServer = new PIServers()[piSystemName];
            this.random = new Random();

        }

        public void Connect()
        {
            piSystem.Connect();
            log.Info($"PI System Connected = {piSystem.ConnectionInfo.IsConnected}");

            piServer.Connect();
            log.Info($"PI Server Connected = {piServer.ConnectionInfo.IsConnected}");
        }

        public void Dispose()
        {
            this.piSystem.Dispose();
            this.piServer.Disconnect();
        }

        internal void CreateAssets(AFSettingsConfig afSettingsConfig)
        {
            log.Info("Creating database...");
            AFDatabase db = this.piSystem.Databases[afSettingsConfig.AFDatabase];
            if (db == null)
            {
                db = this.piSystem.Databases.Add(afSettingsConfig.AFDatabase);
            }

            log.Info("Creating elementTemplate...");
            AFElementTemplate elementTemplate = db.ElementTemplates[afSettingsConfig.ElementTemplate.Name];
            if (elementTemplate == null)
            {
                elementTemplate = db.ElementTemplates.Add(afSettingsConfig.ElementTemplate.Name);
            }

            foreach (AttributeTemplateConfig attributeTemplateConfig in afSettingsConfig.ElementTemplate.AttributeTemplates)
            {
                log.Info($"Creating attributeTemplate {attributeTemplateConfig.Name}...");
                AFAttributeTemplate attributeTemplate = elementTemplate.AttributeTemplates[attributeTemplateConfig.Name];
                if (attributeTemplate == null)
                {
                    attributeTemplate = elementTemplate.AttributeTemplates.Add(attributeTemplateConfig.Name);
                    attributeTemplate.Type = typeConverter[attributeTemplateConfig.Type];
                    attributeTemplate.DataReferencePlugIn = db.PISystem.DataReferencePlugIns["PI Point"];
                    attributeTemplate.ConfigString = GenerateConfigString(attributeTemplate.Name, attributeTemplate.Type);
                    attributeTemplate.DefaultUOM = piSystem.UOMDatabase.UOMs[attributeTemplateConfig.UOM];
                }
            }

            foreach (var stateKeyPair in afSettingsConfig.AFTree)
            {
                string stateName = stateKeyPair.Key;
                log.Info($"Creating state {stateName}...");
                StateConfig stateConfig = stateKeyPair.Value;

                AFElement stateElement = db.Elements[stateName];
                if (stateElement == null)
                {
                    stateElement = db.Elements.Add(stateName);
                }


                foreach (var cityKeyPair in stateConfig)
                {
                    string cityName = cityKeyPair.Key;
                    log.Info($"Creating city {cityName}...");
                    CityConfig cityConfig = cityKeyPair.Value;

                    AFElement cityElement = stateElement.Elements[cityName];
                    if (cityElement == null)
                    {
                        cityElement = stateElement.Elements.Add(cityName);
                    }

                    foreach (var meter in cityConfig.Meters)
                    {
                        string meterName = GetMeterElement(meter.Id);
                        try
                        {
                            CreateMeter(meterName, cityElement, elementTemplate);

                        }
                        catch (Exception e)
                        {
                            log.Error($"Error creating meter {meterName}...", e);
                            System.Threading.Thread.Sleep(5000);
                            CreateMeter(meterName, cityElement, elementTemplate);
                        }
                    }
                }
            }
            db.CheckIn();
        }

        private void CreateMeter(string meterName, AFElement cityElement, AFElementTemplate elementTemplate)
        {
            log.Info($"Creating meter {meterName}...");
            AFElement meterElement = cityElement.Elements[meterName];
            if (meterElement == null)
            {
                meterElement = cityElement.Elements.Add(meterName, elementTemplate);
                CreatePIPoints(meterElement, elementTemplate);
            }
        }

        private void CreatePIPoints(AFElement meterElement, AFElementTemplate elementTemplate)
        {
            IDictionary<string, object> attributeValues = new Dictionary<string, object>();
            attributeValues.Add(PICommonPointAttributes.PointType, "Float32");
            foreach (var attribute in elementTemplate.AttributeTemplates)
            {
                string attributeName = attribute.Name;
                string piPointName = $"{meterElement.Name}_{attributeName}";

                PIPoint piPoint;
                bool pointFound = PIPoint.TryFindPIPoint(piServer, piPointName, out piPoint);
                if (!pointFound)
                {
                    piServer.CreatePIPoint(piPointName, attributeValues);
                }
            }
        }

        private string GetMeterElement(int id)
        {
            int gid = 1000000 + id;
            return $"Meter_{gid}";
        }

        private string GenerateConfigString(string attributeName, Type type)
        {
            string configString = $@"\\%Server%\%Element%_{attributeName};";
            configString += $"pointtype={pointTypeConverter[type]};";
            return configString;
        }
    }
}

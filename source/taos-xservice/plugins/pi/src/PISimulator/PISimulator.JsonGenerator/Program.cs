using Newtonsoft.Json;
using PISimulator.Core.Config;
using System;
using System.Collections.Generic;
using System.IO;

namespace PISimulator.JsonGenerator
{
    class Program
    {
        private static int meterIdCount = 1;
        private static Random random = new Random();
        static void Main(string[] args)
        {
            GenerateDefaultJson();
            GenerateJsonForEdfTest();

        }

        private static void GenerateJsonForEdfTest()
        {
            int numberOfMetersPerCity = 15000;
            AFSettingsConfig afSettingsConfig = new AFSettingsConfig();
            afSettingsConfig.AFDatabase = "EdfMeters";
            afSettingsConfig.PIPointPrefix = "Meters_";
            afSettingsConfig.ElementTemplate = new ElementTemplateConfig();
            afSettingsConfig.ElementTemplate.Name = "MeterTemplate";
            afSettingsConfig.ElementTemplate.AttributeTemplates = new List<AttributeTemplateConfig>()
            {
                new AttributeTemplateConfig()
                {
                      Name = "Current",
                      Type =  "double",
                      UOM =  "A"
                },
                new AttributeTemplateConfig()
                {
                      Name = "Voltage",
                      Type =  "double",
                      UOM =  "V"
                }
            };
            afSettingsConfig.AFTree = new AFTreeConfig();
            var californiaConfig = new StateConfig();
            californiaConfig.Add("San Francisco", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("San Diego", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("Los Angeles", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("San Jose", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("Oakland", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("Hayward", GenerateCityConfig(numberOfMetersPerCity));
            afSettingsConfig.AFTree.Add("California", californiaConfig);
            string str = JsonConvert.SerializeObject(afSettingsConfig, Formatting.Indented);
            string fileName = "AFSettingsGeneratedEdf.json";
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
            File.WriteAllText(fileName, str);
        }

        private static void GenerateDefaultJson()
        {
            int numberOfMetersPerCity = 100;
            AFSettingsConfig afSettingsConfig = new AFSettingsConfig();
            afSettingsConfig.AFDatabase = "Meters";
            afSettingsConfig.PIPointPrefix = "Meters_";
            afSettingsConfig.ElementTemplate = new ElementTemplateConfig();
            afSettingsConfig.ElementTemplate.Name = "MeterTemplate";
            afSettingsConfig.ElementTemplate.AttributeTemplates = new List<AttributeTemplateConfig>()
            {
                new AttributeTemplateConfig()
                {
                      Name = "Current",
                      Type =  "double",
                      UOM =  "A"
                },
                new AttributeTemplateConfig()
                {
                      Name = "Voltage",
                      Type =  "double",
                      UOM =  "V"
                }
            };
            afSettingsConfig.AFTree = new AFTreeConfig();
            var californiaConfig = new StateConfig();
            californiaConfig.Add("San Francisco", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("San Diego", GenerateCityConfig(numberOfMetersPerCity));
            californiaConfig.Add("Los Angeles", GenerateCityConfig(numberOfMetersPerCity));
            var Texas = new StateConfig();
            Texas.Add("Dallas", GenerateCityConfig(numberOfMetersPerCity));
            Texas.Add("Houston", GenerateCityConfig(numberOfMetersPerCity));
            Texas.Add("Austin", GenerateCityConfig(numberOfMetersPerCity));
            afSettingsConfig.AFTree.Add("California", californiaConfig);
            afSettingsConfig.AFTree.Add("Texas", Texas);
            string str = JsonConvert.SerializeObject(afSettingsConfig, Formatting.Indented);
            string fileName = "AFSettingsGenerated.json";
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
            File.WriteAllText(fileName, str);
        }

        private static CityConfig GenerateCityConfig(int numberOfMeters)
        {
            CityConfig cityConfig = new CityConfig();
            cityConfig.Meters = new List<MeterConfig>();
            for (int i = 0; i < numberOfMeters; i++)
            {
                cityConfig.Meters.Add(GenerateMeterConfig());
            }
          
            return cityConfig;
        }

        private static MeterConfig GenerateMeterConfig()
        {
            return new MeterConfig()
            {
                Id = meterIdCount++,
                Type = GenerateRandomDataTypeEnum(),
                TimePeriod = GenerateRandomInt(120),
                Current = new MeterAttributeConfig()
                {
                    LowerLimit = GenerateRandomDouble(4, 1),
                    HigherLimit = GenerateRandomDouble(28, 2),
                    RandomNoise = GenerateRandomDouble(2, 1),
                    OffsetPhase = GenerateRandomDouble(2, 1),
                },
                Voltage = new MeterAttributeConfig()
                {
                    LowerLimit = GenerateRandomDouble(114, 2),
                    HigherLimit = GenerateRandomDouble(126, 2),
                    RandomNoise = GenerateRandomDouble(2, 1),
                    OffsetPhase = GenerateRandomDouble(2, 1),
                }
            };
        }

        private static double GenerateRandomDouble(double value, double noise)
        {
            double randomValue = 2*(random.NextDouble() - 0.5);
            return Math.Round(value + noise * randomValue, 2);
        }

        private static int GenerateRandomInt(int value)
        {
            double v = random.NextDouble();
            if (v < 0.3333)
            {
                return value - 1;
            }
            else if (v > 0.66666)
            {
                return value + 1;
            }
            else
            {
                return value;
            }
        }

        private static DataTypeEnum GenerateRandomDataTypeEnum()
        {
       
            double v = random.NextDouble();
            if (v < 0.3333)
            {
                return DataTypeEnum.SinusoidWave;
            }
            else if (v > 0.66666)
            {
                return DataTypeEnum.TriangleWave;
            }
            else
            {
                return DataTypeEnum.SquareWave;
            }
        }
    }
}

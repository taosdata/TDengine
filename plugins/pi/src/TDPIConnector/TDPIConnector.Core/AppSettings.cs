using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Nett; // Nett is a popular TOML library for C#
using System.Text;

namespace TDPIConnector.Core
{
    public static class AppSettings
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public class TomlConfig {
            public int MaxWaitLen { get; set; } = 1000;
            public int UpdateInterval { get; set; } = 10000;
            public int MaxBackfillRangeDays { get; set; } = 1;
            public string PIServerName { get; set; }
            public string PISystemName { get; set; }
            public string AFDatabaseName { get; set; }
            public int PIDataPipesInstances { get; set; } = 1;
            public int AFDataPipesInstances { get; set; } = 1;
            public string IPCStream { get; set; }
            public string SQLAPI { get; set; }
            public string AFTreeTagName { get; set; } = "location";
            public int HttpMaxRetryTimes { get; set; } = 3;
            public string TDDataBase { get; set; } = "pi";
            public List<string> TemplateForPIPoint { get; set; }
            public List<string> TemplateForAFElement { get; set; }
            public List<string> PointList { get; set; }

            // not support
            public string PIServerUser { get; set; }
            public string PIServerPassword { get; set; }
            public string PIServerDomain { get; internal set; }
            public bool FromTDengineLastTime { get; set; }
            public bool ToTDengineFirstTime { get; set; }
            public DateTime BackfillStartTime { get; set; } = DateTime.MinValue;
            public DateTime BackfillEndTime { get; set; } = DateTime.MaxValue;

            public string ConfigString()
            {
                var sb = new StringBuilder();
                sb.AppendLine($"MaxWaitLen={MaxWaitLen}");
                sb.AppendLine($"UpdateInterval={UpdateInterval}");
                sb.AppendLine($"MaxBackfillRangeDays={MaxBackfillRangeDays}");
                sb.AppendLine($"PIServerName={PIServerName}");
                sb.AppendLine($"PISystemName={PISystemName}");
                sb.AppendLine($"AFDatabaseName={AFDatabaseName}");
                sb.AppendLine($"PIDataPipesInstances={PIDataPipesInstances}");
                sb.AppendLine($"AFDataPipesInstances={AFDataPipesInstances}");
                sb.AppendLine($"IPCStream={IPCStream}");
                sb.AppendLine($"SQLAPI={SQLAPI}");
                sb.AppendLine($"FromTDengineLastTime={FromTDengineLastTime}");
                sb.AppendLine($"ToTDengineFirstTime={ToTDengineFirstTime}");
                sb.AppendLine($"BackfillStartTime={BackfillStartTime}");
                sb.AppendLine($"BackfillEndTime={BackfillEndTime}");

                if (TemplateForPIPoint != null && TemplateForPIPoint.Any())
                {
                    sb.AppendLine($"TemplateForPIPoint={string.Join(", ", TemplateForPIPoint)}");
                }

                if (TemplateForAFElement != null && TemplateForAFElement.Any())
                {
                    sb.AppendLine($"TemplateForAFElement={string.Join(", ", TemplateForAFElement)}");
                }
                if (PointList != null && PointList.Any())
                {
                    sb.AppendLine($"PointList={string.Join(", ", PointList)}");
                }
                return sb.ToString();
            }

            public void SetBackfillOption(bool backfillToFirstRecorded, bool backfillFromLastRecorded, DateTime start, DateTime end)
            {
                FromTDengineLastTime = backfillFromLastRecorded;
                ToTDengineFirstTime = backfillToFirstRecorded;
                BackfillStartTime = start;
                BackfillEndTime = end;
            }
        }

        public static void Init(string tomlConfigFile)
        {
            tomlConfig = new TomlConfig();
            if (tomlConfigFile != null && tomlConfigFile != "") {
                tomlConfig = Toml.ReadFile<TomlConfig>(tomlConfigFile);
                log.Info($"toml file: {tomlConfig.ConfigString()}");
            }

            if (string.IsNullOrEmpty(tomlConfig.PIServerDomain))
            {
                tomlConfig.PIServerDomain = null;
            }
            if (tomlConfigFile == null || tomlConfigFile == "")
            {
                TaosXEnabled = false;
                tomlConfig.UpdateInterval = GetIntegerFromAppSettings("UpdateInterval");
                tomlConfig.PISystemName = GetStringFromAppSettings("PISystemName");
                tomlConfig.PIServerName = GetStringFromAppSettings("PIServerName");
                tomlConfig.PIServerUser = GetStringFromAppSettings("PIServerUser");
                tomlConfig.PIServerDomain = GetStringFromAppSettings("PIServerDomain");
                if (string.IsNullOrEmpty(tomlConfig.PIServerDomain))
                {
                    tomlConfig.PIServerDomain = null;
                }
                tomlConfig.PIServerPassword = GetStringFromAppSettings("PIServerPassword");
                tomlConfig.AFDatabaseName = GetStringFromAppSettings("AFDatabaseName");
                tomlConfig.AFDataPipesInstances = GetIntegerFromAppSettings("AFDataPipesInstances", 1);
                tomlConfig.PIDataPipesInstances = GetIntegerFromAppSettings("PIDataPipesInstances", 1);
                tomlConfig.MaxBackfillRangeDays = GetIntegerFromAppSettings("MaxBackfillRangeDays", 1);
                tomlConfig.TDDataBase = GetStringFromAppSettings("TDEnginePIDatabase");

                TDEngineHost = GetStringFromAppSettings("TDEngineHost");
                TDEnginePort = GetIntegerFromAppSettings("TDEnginePort");
                TDEngineUsername = GetStringFromAppSettings("TDEngineUsername");
                TDEnginePassword = GetStringFromAppSettings("TDEnginePassword");
                TDEngineToken = GetStringFromAppSettings("TDEngineToken");

                try
                {
                    tomlConfig.PointList = System.IO.File.ReadLines(AppDomain.CurrentDomain.BaseDirectory + "Points.csv").Distinct().ToList();
                    tomlConfig.TemplateForPIPoint = System.IO.File.ReadLines(AppDomain.CurrentDomain.BaseDirectory + "ElementTemplates1.csv").Distinct().ToList();
                    tomlConfig.TemplateForAFElement = System.IO.File.ReadLines(AppDomain.CurrentDomain.BaseDirectory + "ElementTemplates2.csv").Distinct().ToList();
                }
                catch (Exception)
                {
                    //throw;
                }
            } 

            TDEnginePITablesPrefix = GetStringFromAppSettings("TDEnginePITablesPrefix");
            if (TDEnginePITablesPrefix == null)
            {
                TDEnginePITablesPrefix = string.Empty;
            }
            WebBaseUrl = GetStringFromAppSettings("WebBaseUrl");
            WebBasePort = GetIntegerFromAppSettings("WebBasePort", 80);
            WebMaxPIEvents = GetIntegerFromAppSettings("WebMaxPIEvents", 5);
            WebMaxTDEngineHttpResponses = GetIntegerFromAppSettings("WebMaxTDEngineHttpResponses", 5);
            WebMonitoringEventsEnabled = GetBooleanFromAppSettings("WebMonitoringEventsEnabled", false);


            if (TDEnginePITablesPrefix == null)
            {
                TDEnginePITablesPrefix = string.Empty;
            }
        }
        public static string TDEngineHost { get; internal set; }
        public static int TDEnginePort { get; internal set; }
        public static string TDEngineUsername { get; internal set; }
        public static string TDEnginePassword { get; internal set; }
        public static string TDEngineToken { get; internal set; }
        public static string TDEnginePITablesPrefix { get; internal set; }
        public static string WebBaseUrl { get; internal set; }
        public static int WebBasePort { get; internal set; }
        public static int WebMaxTDEngineHttpResponses { get; internal set; }
        public static int WebMaxPIEvents { get; internal set; }
        public static bool WebMonitoringEventsEnabled { get; private set; }

        public static bool TaosXEnabled { get; private set; } = true;
        public static TomlConfig tomlConfig { get; private set; }

        private static string GetStringFromAppSettings(string propertyName)
        {
            if (ConfigurationManager.AppSettings[propertyName] != null)
            {
                return ConfigurationManager.AppSettings[propertyName].Trim();
            }
            else
            {
                return null;
            }
        }

        private static bool GetBooleanFromAppSettings(string propertyName, bool? defaultValue = null)
        {
            if (ConfigurationManager.AppSettings[propertyName] != null)
            {
                return Convert.ToBoolean(ConfigurationManager.AppSettings[propertyName]);
            }
            else if (defaultValue != null)
            {
                return Convert.ToBoolean(defaultValue);
            }
            else
            {
                throw new Exception("Property not found");
            }
        }

        private static int GetIntegerFromAppSettings(string propertyName, int? defaultValue = null)
        {
            if (ConfigurationManager.AppSettings[propertyName] != null)
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings[propertyName]);
            }
            else if (defaultValue != null)
            {
                return Convert.ToInt32(defaultValue);
            }
            else
            {
                throw new Exception("Property not found");
            }
        }

        private static double GetDoubleFromAppSettings(string propertyName)
        {
            if (ConfigurationManager.AppSettings[propertyName] != null)
            {
                return Convert.ToDouble(ConfigurationManager.AppSettings[propertyName]);
            }
            else
            {
                throw new Exception("Property not found");
            }
        }
    }
}


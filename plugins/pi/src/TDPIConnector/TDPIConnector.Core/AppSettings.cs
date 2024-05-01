using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Tomlyn;

namespace TDPIConnector.Core
{
    public static class AppSettings
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public class TomlConfig {
            public string LogLevel { get; set; }
            public int MaxWaitLen { get; set; } = 1000;
            public int UpdateInterval { get; set; } = 1000;
            public int MaxBackfillRangeDays { get; set; } = 1; // uit: Minutes
            public string PIServerName { get; set; }
            public string PISystemName { get; set; }
            public string AFDatabaseName { get; set; }
            public int PIDataPipesInstances { get; set; } = 1;
            public int AFDataPipesInstances { get; set; } = 10;
            public string IPCStream { get; set; }
            public string SQLAPI { get; set; }
            public string AFTreeTagName { get; set; } = "path";
            public int HttpMaxRetryTimes { get; set; } = 3;
            public string TDDataBase { get; set; } = "pi";
            public List<string> TemplateForPIPoint { get; set; } = new List<string>();
            public List<string> TemplateForAFElement { get; set; } = new List<string>();
            public List<string> PointList { get; set; }
            public List<string> ElementList { get; set; } = new List<string>();
            public List<string> ElementIDList { get; set; } = new List<string>();

            // not support
            public string PIServerUser { get; set; }
            public string PIServerPassword { get; set; }
            public string PIServerDomain { get; internal set; }
            public bool FromTDengineLastTime { get; set; }
            public bool ToTDengineFirstTime { get; set; }
            public DateTimeOffset BackfillStartTime { get; set; } = DateTimeOffset.MinValue;
            public DateTimeOffset BackfillEndTime { get; set; } = DateTimeOffset.MaxValue;

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
                    if (ElementIDList.Count() <= 10)
                    {
                        sb.AppendLine($"PointList={string.Join(", ", PointList)}");
                    }
                    else
                    {
                        sb.AppendLine($"PointList count: {ElementIDList.Count()}");
                    }
                    
                }
                if (ElementIDList != null && ElementIDList.Any())
                {
                    if (ElementIDList.Count() <= 10)
                    {
                        sb.AppendLine($"ElementIDList={string.Join(", ", ElementIDList)}");
                    }
                    else
                    {
                        sb.AppendLine($"ElementIDList count: {ElementIDList.Count()}");
                    }
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

                string fileData = System.IO.File.ReadAllText(tomlConfigFile);

                var tomlOption = new TomlModelOptions();
                tomlOption.IgnoreMissingProperties = true;
                tomlOption.ConvertPropertyName = (propertyName) =>
                {
                    return propertyName;
                };
                tomlConfig = Toml.ToModel<TomlConfig>(fileData, null, tomlOption);
                log.Info($"toml file: {tomlConfig.ConfigString()}");
            }

            if (string.IsNullOrEmpty(tomlConfig.PIServerDomain))
            {
                tomlConfig.PIServerDomain = null;
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
            BackfillQuitWait = GetIntegerFromAppSettings("BackfillQuitWait", 60);
            MaxEventCountObserverFetchOnce = GetIntegerFromAppSettings("MaxEventCountObserverFetchOnce", 10000);

            if (TDEnginePITablesPrefix == null)
            {
                TDEnginePITablesPrefix = string.Empty;
            }

            if (!string.IsNullOrEmpty(tomlConfig.LogLevel)) {
                log4net.Repository.ILoggerRepository repository = log4net.LogManager.GetRepository();
                log4net.Repository.Hierarchy.Hierarchy hier = (log4net.Repository.Hierarchy.Hierarchy)repository;
                hier.Root.Level = hier.LevelMap[tomlConfig.LogLevel];
                ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty);

                log.Info($"Reset log level to {tomlConfig.LogLevel}.");
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
        public static int BackfillQuitWait { get; internal set; }
        public static int MaxEventCountObserverFetchOnce { get; internal set; }
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


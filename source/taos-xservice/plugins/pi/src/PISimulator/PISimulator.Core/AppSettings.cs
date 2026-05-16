using Newtonsoft.Json;
using PISimulator.Core.Config;
using System;
using System.Configuration;
using System.IO;

namespace PISimulator.Core
{
    public static class AppSettings
    {
        public static void Init()
        {
            UpdateInterval = GetIntegerFromAppSettings("UpdateInterval");
            PISystemName = GetStringFromAppSettings("PISystemName");
            PIServerName = GetStringFromAppSettings("PIServerName");
            string text = File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "\\AFSettings.json");
            AFSettingsConfig = JsonConvert.DeserializeObject<AFSettingsConfig>(text);
        }

        public static int UpdateInterval { get; internal set; }
        public static string PISystemName { get; private set; }
        public static string PIServerName { get; private set; }
        public static AFSettingsConfig AFSettingsConfig { get; private set; }

        private static bool GetBooleanFromAppSettings(string propertyName)
        {
            string value = GetStringFromAppSettings(propertyName);
            return value.Trim().ToLower() == "true";
        }

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

        private static int GetIntegerFromAppSettings(string propertyName)
        {
            if (ConfigurationManager.AppSettings[propertyName] != null)
            {
                return Convert.ToInt32(ConfigurationManager.AppSettings[propertyName]);
            }
            else
            {
                throw new Exception("Property not found");
            }
        }
    }
}


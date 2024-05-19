using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using TDPIConnector.PI;

namespace TDPIConnector.Core
{
    class PIConfigChecker
    {
        private PIServerManager piServerManager;
        private PISystemManager pISystemManager;

        public class CheckReslut {
            public string version;
            public bool avaliable;
            public string since;
            public List<string> items = new List<string>();
        }

        public PIConfigChecker(PIServerManager piServerManager, PISystemManager pISystemManager)
        {
            this.piServerManager = piServerManager;
            this.pISystemManager = pISystemManager;
        }

        internal string Check()
        {
            var checkResult = new CheckReslut();
            checkResult.version = PISystemManager.GetPISDKInfo();
            if (AppSettings.tomlConfig.AFDatabaseName != null && AppSettings.tomlConfig.AFDatabaseName != "" && !DBValid(AppSettings.tomlConfig.AFDatabaseName))
            {
                checkResult.avaliable = false;
                checkResult.since = "AF Database not found";
            }
            else {
                var notExistPoints = CheckPointsExistence(AppSettings.tomlConfig.PointList);
                foreach (string point in notExistPoints)
                {
                    checkResult.items.Add($"point {point} not exist");
                }

                if ((pISystemManager == null || AppSettings.tomlConfig.AFDatabaseName == "") &&
                    ( AppSettings.tomlConfig.TemplateForPIPoint.Count() > 0 || AppSettings.tomlConfig.TemplateForAFElement.Count() > 0 ) )
                {
                    checkResult.items.Add($"AF Server not config correct, template can't used.");
                }
                else
                {
                    var notExistTemplate = CheckTemplatesExistence(AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.TemplateForAFElement);
                    foreach (string template in notExistTemplate)
                    {
                        if (checkResult.items.Contains(template)) continue;
                        checkResult.items.Add($"template {template} not exist");
                    }
                    notExistTemplate = CheckTemplatesExistence(AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.TemplateForPIPoint);
                    foreach (string template in notExistTemplate)
                    {
                        if (checkResult.items.Contains(template)) continue;
                        checkResult.items.Add($"template {template} not exist");
                    }
                    var noTemplateElements = CheckElementsNoTemplateValid(AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.ElementList);
                    foreach (string elementName in noTemplateElements)
                    {
                        if (checkResult.items.Contains(elementName)) continue;
                        checkResult.items.Add($"ElementOfNoTemplate invalid: {elementName}");
                    }
                }
                if (checkResult.items.Count() > 0)
                {
                    checkResult.avaliable = false;
                    checkResult.since = "Some points or templates do not exist";
                }
                else
                {
                    checkResult.avaliable = true;
                    checkResult.since = "Config is ok";
                }
            }
           
            var json = JsonConvert.SerializeObject(checkResult);
            return json;
        }
        internal List<string> CheckPointsExistence(List<string> pointNames)
        {
            List<string> result = new List<string>();
            if (pointNames == null) return result;
            foreach (string pointName in pointNames)
            {
                try
                {
                    piServerManager.FindPIPoint(pointName);
                }
                catch (Exception)
                {
                    result.Add(pointName);
                }
            }

            return result;
        }
        internal List<string> CheckTemplatesExistence(string afDBName, List<string> templateNames)
        {
            List<string> result = new List<string>();
            if (templateNames == null) return result;
            foreach (string templateName in templateNames)
            {
                try
                {
                    var template = pISystemManager.GetElementTemplates(afDBName, templateName);
                    if(template.Count() == 0) result.Add(templateName);
                }
                catch (Exception)
                {
                    result.Add(templateName);
                }
            }

            return result;
        }

        internal List<string> CheckElementsNoTemplateValid(string afDBName, List<string> elementNames)
        {
            List<string> result = new List<string>();
            if (elementNames == null) return result;
            foreach (string elementName in elementNames)
            {
                if (!ElementNoTemplateValid(afDBName, elementName)) {
                    result.Add(elementName);
                }
            }
            return result;
        }
        internal bool ElementNoTemplateValid(string afDBName, string elementName)
        {
            try
            {
                var wrappers = pISystemManager.GetElementByName(afDBName, elementName);
                if (wrappers.Count() == 0) return false;
                foreach (var element in wrappers) {
                    if (element.hasTemplate())
                    {
                        return false;
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        internal bool DBValid(string afDBName)
        {
            OSIsoft.AF.AFDatabase db = null;
            try {
                db = pISystemManager.GetAFDatabase(afDBName);
            } catch (Exception) {
                return false;
            }
            return db != null;
        }

        static public string buildConnectFailedInfo() {
            var result = new CheckReslut();
            result.version = PISystemManager.GetPISDKInfo();
            result.avaliable = false;
            result.since = "DataAchive or AF Server cannot connect.";
            string info = JsonConvert.SerializeObject(result);
            return info;
        }
    }
}

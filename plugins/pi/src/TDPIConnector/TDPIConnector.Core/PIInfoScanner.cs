using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using TDPIConnector.PI;

namespace TDPIConnector.Core
{
    class PIInfoScanner
    {
        private PIServerManager piServerManager;
        private PISystemManager pISystemManager;

        class PIInfo {
            public List<string> pointsName;
            public List<string> templateName;
        }

        public PIInfoScanner(PIServerManager piServerManager, PISystemManager pISystemManager)
        {
            this.piServerManager = piServerManager;
            this.pISystemManager = pISystemManager;
        }

        internal string GetInfo(string pointFilter)
        {
            var points = piServerManager.FindPIPoints(pointFilter);
            var templates = pISystemManager.GetElementTemplates(AppSettings.tomlConfig.AFDatabaseName);
            var piInfo = new PIInfo();
            piInfo.pointsName = points.Select(p => p.Name).ToList();
            piInfo.templateName = templates.Select(t => t.Name).ToList();

            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }
    }
}

using log4net;
using OSIsoft.AF;
using System;
using System.Net;

namespace PISimulator.PI
{
    public class PISystemManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PISystem piSystem;
        private string piSystemUserName;
        private string piSystemPassword;

        public PISystemManager(string piSystemName)
        {
            this.piSystem = new PISystems()[piSystemName];

        }

        public void Connect()
        {
            if (string.IsNullOrEmpty(piSystemUserName) || string.IsNullOrEmpty(piSystemPassword))
            {
                piSystem.Connect();
            }
            else
            {
                piSystem.Connect(new NetworkCredential(piSystemUserName, piSystemPassword));
            }
            log.Info($"PI System Connected = {piSystem.ConnectionInfo.IsConnected}");
        }

        public void Dispose()
        {
            this.piSystem.Dispose();
        }
    }
}

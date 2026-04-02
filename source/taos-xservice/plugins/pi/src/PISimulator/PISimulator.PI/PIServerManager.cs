using log4net;
using OSIsoft.AF.PI;
using System;
using System.Net;

namespace PISimulator.PI
{
    public class PIServerManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PIServer piServer;
        private string piServerUserName;
        private string piServerPassword;

        public PIServerManager(string piServerName)
        {
            this.piServer = new PIServers()[piServerName];

        }

        public void Connect()
        {
            if (string.IsNullOrEmpty(piServerUserName) || string.IsNullOrEmpty(piServerPassword))
            {
                piServer.Connect();
            }
            else
            {
                piServer.Connect(new NetworkCredential(piServerUserName, piServerPassword));
            }
            log.Info($"PI Data Archive Connected = {piServer.ConnectionInfo.IsConnected}");
        }

        public void Dispose()
        {
            this.piServer.Disconnect();
        }
    }
}

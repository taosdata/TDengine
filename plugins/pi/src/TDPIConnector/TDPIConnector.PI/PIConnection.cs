using OSIsoft.AF.PI;

namespace TDPIConnector.PI
{
    public class PIConnection
    {
        public PIConnection(PIConnectionInfo connectionInfo)
        {
            this.Host = connectionInfo.Host;
            this.IsConnected = connectionInfo.IsConnected;
            this.Port = connectionInfo.Port;
            this.ServerVersion = connectionInfo.PIServer.ServerVersion;
        }

        public string Host { get; }
        public bool IsConnected { get; }
        public int Port { get; }
        public string ServerVersion { get; }
    }
}

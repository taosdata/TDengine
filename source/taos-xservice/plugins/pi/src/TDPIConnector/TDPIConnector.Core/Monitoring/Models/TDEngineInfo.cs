namespace TDPIConnector.Core.Monitoring
{
    public class TDEngineInfo
    { 
        public string Host { get; }
        public int Port { get; }
        public string Database { get; }
        public string Version { get; }

        public TDEngineInfo(string host, int port, string database, string version)
        {
            this.Host = host;
            this.Port = port;
            this.Database = database;
            this.Version = version;
        }
    }
}
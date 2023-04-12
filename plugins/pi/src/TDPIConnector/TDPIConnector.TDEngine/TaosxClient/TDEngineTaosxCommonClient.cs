using log4net;
using System;
using System.Collections.Generic;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.TDEngine.Helper;
using System.Threading.Tasks;
using System.Linq;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDEngineTaosxCommonClient : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private TDEngineTaosSocket taosxSocket;

        public TDEngineTaosxCommonClient(string hostname, int port) {
            taosxSocket = new TDEngineTaosSocket(hostname, port, true);
        }
        // write data
        public void Connect()
        {
            try
            {
                taosxSocket.Connect();
                string version = getServerVersion();
                // TODO callback
                // DoServerVersionReceived(version);
            }
            catch (Exception)
            {
                throw new Exception("Could not connect to Taosx. Please check the settings on the .config file.");
            }
        }

        private string getServerVersion() {
            return "";
        }

        public Task<TDValue> GetLastPIValue(string database, string pointName)
        {
            // TODO 
            return Task.FromResult<TDValue>(null);
        }

        public  void Dispose()
        {

        }

        public async Task<Dictionary<string, DateTime>> GetLastPIValues(string database, List<string> tableNames, IEnumerable<string> sTableNames)
        {
            sTableNames = sTableNames.Select(st => st.ToTDEngineNamingPattern()).ToList();
            Dictionary<string, DateTime> lastValueTimestamps = new Dictionary<string, DateTime>();
            Dictionary<string, DateTime> allLastValueTimestamps = new Dictionary<string, DateTime>();

            foreach (var STableName in sTableNames)
            {
                break;
                string sqlCommand = $"SELECT tbname, LAST_ROW(*) FROM {database}.{STableName} PARTITION BY TBNAME;";
                var resData = taosxSocket.SendData(System.Text.Encoding.Default.GetBytes(sqlCommand));
                TDEngineResponse resp = parseLastPIValues(resData);
                if (resp != null && resp.Data != null) {
                    foreach (var dataItem in resp.Data)
                    {
                        allLastValueTimestamps.Add(dataItem[0], DateTime.Parse(dataItem[1]));
                    }
                }
            }

            foreach (var tableName in tableNames)
            {
                string tdEngineTableName = TDEngineProxy.GetFullTableName(tableName).ToTDEngineNamingPattern();
                if (allLastValueTimestamps.ContainsKey(tdEngineTableName))
                {
                    lastValueTimestamps.Add(tableName, allLastValueTimestamps[tdEngineTableName]);
                }
                else
                {
                    lastValueTimestamps.Add(tableName, DateTime.MinValue);
                }
            }
            return lastValueTimestamps;
        }

        private TDEngineResponse parseLastPIValues(byte[] resp)
        {
            if (resp == null || resp.Length == 0) {
                return new TDEngineResponse();
            }
            log.Debug($"last PI value:{System.Text.Encoding.UTF8.GetString(resp)}");
            return null;
        }

        internal Task<TDValue> GetFirstPIValue(string database, string pointName)
        {
            return Task.FromResult<TDValue>(null);
        }
    }
}

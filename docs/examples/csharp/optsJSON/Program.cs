using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class OptsJsonExample
    {
        public static void Main(string[] args)
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                client.Exec("CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
                client.Exec("use test");
                string[] lines =
                {
                    "[{\"metric\": \"meters.current\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                    " {\"metric\": \"meters.voltage\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}, " +
                    "{\"metric\": \"meters.current\", \"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                    " {\"metric\": \"meters.voltage\", \"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}]"
                };
                client.SchemalessInsert(lines, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            }
        }
    }
}
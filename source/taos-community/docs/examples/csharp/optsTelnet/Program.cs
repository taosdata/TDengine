using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class OptsTelnetExample
    {
        public static void Main(string[] args)
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                client.Exec("CREATE DATABASE test WAL_RETENTION_PERIOD 3600");
                client.Exec("USE test");
                string[] lines = {
                    "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
                    "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
                    "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
                    "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
                    "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
                    "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
                    "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
                    "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
                };
                client.SchemalessInsert(lines,
                    TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            }
        }
    }
}

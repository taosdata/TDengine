using TDengine.Driver;
using TDengine.Driver.Client;

namespace WSSchemaless
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var builder =
                new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                client.Exec("create database sml");
                client.Exec("use sml");
                var influxDBData =
                    "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
                client.SchemalessInsert(new string[] { influxDBData },
                    TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS, 0, ReqId.GetReqId());
                var telnetData = "stb0_0 1626006833 4 host=host0 interface=eth0";
                client.SchemalessInsert(new string[] { telnetData },
                    TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
                var jsonData =
                    "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";
                client.SchemalessInsert(new string[] { jsonData }, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS, 0, ReqId.GetReqId());
            }
        }
    }
}
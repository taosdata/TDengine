using System;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace WSFailoverQuery
{
    internal class Program
    {
        private const string DatabaseName = "power_failover_query";
        private const string StableName = "meters";
        private const string TableName = "d1001";
        private const string Hosts = "127.0.0.1:6042,127.0.0.1:6043";

        public static void Main(string[] args)
        {
            var builder = new ConnectionStringBuilder(
                "protocol=WebSocket;" +
                $"host={Hosts};" +
                "useSSL=false;" +
                "username=root;" +
                "password=taosdata;" +
                "autoReconnect=true;" +
                "reconnectRetryCount=10;" +
                "reconnectIntervalMs=200");

            using (var client = DbDriver.Open(builder))
            {
                PrepareData(client);
                Console.WriteLine("Failover query loop started. Stop one adapter to verify failover.");

                while (true)
                {
                    try
                    {
                        using (var rows = client.Query(
                                   $"SELECT ts,current,voltage,phase FROM {DatabaseName}.{TableName} ORDER BY ts DESC LIMIT 1"))
                        {
                            if (rows.Read())
                            {
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} query ok => ts={((DateTime)rows.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, current={rows.GetValue(1)}, voltage={rows.GetValue(2)}, phase={rows.GetValue(3)}");
                            }
                            else
                            {
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} query ok => no rows");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} query failed => {ex.Message}");
                    }

                    // System.Threading.Thread.Sleep(1000);
                }
            }
        }

        private static void PrepareData(ITDengineClient client)
        {
            client.Exec($"CREATE DATABASE IF NOT EXISTS {DatabaseName}");
            client.Exec($"CREATE STABLE IF NOT EXISTS {DatabaseName}.{StableName} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
            client.Exec(
                $"INSERT INTO {DatabaseName}.{TableName} USING {DatabaseName}.{StableName} TAGS(1,'manual.failover') VALUES(now,11.5,219,0.30)");
        }
    }
}

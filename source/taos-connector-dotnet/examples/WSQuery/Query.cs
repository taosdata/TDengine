using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace WSQuery
{
    internal class Query
    {
        public static void Main(string[] args)
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"create database power");
                    client.Exec($"use power");
                    client.Exec(
                        "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                    string insertQuery =
                        "INSERT INTO " +
                        "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                        "VALUES " +
                        "('2023-10-03 14:38:05.000', 10.30000, 219, 0.31000) " +
                        "('2023-10-03 14:38:15.000', 12.60000, 218, 0.33000) " +
                        "('2023-10-03 14:38:16.800', 12.30000, 221, 0.31000) " +
                        "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                        "VALUES " +
                        "('2023-10-03 14:38:16.650', 10.30000, 218, 0.25000) " +
                        "power.d1003 USING power.meters TAGS(2,'California.LosAngeles') " +
                        "VALUES " +
                        "('2023-10-03 14:38:05.500', 11.80000, 221, 0.28000) " +
                        "('2023-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                        "power.d1004 USING power.meters TAGS(3,'California.LosAngeles') " +
                        "VALUES " +
                        "('2023-10-03 14:38:05.000', 10.80000, 223, 0.29000) " +
                        "('2023-10-03 14:38:06.500', 11.50000, 221, 0.35000)";
                    client.Exec(insertQuery);
                    string query = "SELECT * FROM meters";
                    using (var rows = client.Query(query))
                    {
                        while (rows.Read())
                        {
                            Console.WriteLine(
                                $"{((DateTime)rows.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {rows.GetValue(1)}, {rows.GetValue(2)}, {rows.GetValue(3)}, {rows.GetValue(4)}, {Encoding.UTF8.GetString((byte[])rows.GetValue(5))}");
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    throw;
                }
            }
        }
    }
}
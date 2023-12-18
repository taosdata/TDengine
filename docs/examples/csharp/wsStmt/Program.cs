using System;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Examples
{
    public class WSStmtExample
    {
        public static void Main(string[] args)
        {
            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"create database power");
                    client.Exec(
                        "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                    using (var stmt = client.StmtInit())
                    {
                        stmt.Prepare(
                            "Insert into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(?,?,?,?)");
                        var ts = new DateTime(2023, 10, 03, 14, 38, 05, 000);
                        stmt.BindRow(new object[] { ts, (float)10.30000, (int)219, (float)0.31000 });
                        stmt.AddBatch();
                        stmt.Exec();
                        var affected = stmt.Affected();
                        Console.WriteLine($"affected rows: {affected}");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
    }
}
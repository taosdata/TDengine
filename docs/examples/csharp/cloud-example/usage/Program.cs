using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Cloud.Examples
{
    public class UsageExample
    {
        static void Main(string[] args)
        {
            var cloudEndPoint = Environment.GetEnvironmentVariable("CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("CLOUD_TOKEN");
            var connectionString = $"protocol=WebSocket;host={cloudEndPoint};port=443;useSSL=true;token={cloudToken};";
            // Connect to TDengine server using WebSocket
            var builder = new ConnectionStringBuilder(connectionString);

            try
            {
               // Open connection with using block, it will close the connection automatically
               using (var client = DbDriver.Open(builder))
               {
                  InsertData(client);
                  SelectData(client);
              }
           }catch (TDengineError e)
           {
              // handle TDengine error
              Console.WriteLine(e.Message);
              throw;
           }
           catch (Exception e)
           {
              // handle other exceptions
              Console.WriteLine(e.Message);
              throw;
           }
        }


        public static void InsertData(ITDengineClient client)
        {
            string createTable = "CREATE STABLE if not exists test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)";
            string insertData = "INSERT INTO " +
                                "test.d1001 USING test.meters1 TAGS('California.SanFrancisco', 1) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000)" +
                                "test.d1002 USING test.meters1 TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)" +
                                "test.d1003 USING test.meters1 TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)" +
                                "test.d1004 USING test.meters1 TAGS('California.LosAngeles', 4) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ";

            // create stable under database named 'test'
            var affected =  client.Exec(createTable);
            Console.WriteLine($"Create stable meters, affected rows: {affected}");
            // insert data into the table created in previous step.
            affected = client.Exec(insertData);
            Console.WriteLine("insert " + affected + " rows to test.meters successfully.");
        }

        public static void SelectData(ITDengineClient client)
        {
            string selectTable = "select * from test.meters";
            using (var rows = client.Query(selectTable))
            {
               while (rows.Read())
               {
                  var ts = (DateTime)rows.GetValue(0);
                  var current = (float)rows.GetValue(1);
                  var voltage = (int)rows.GetValue(2);
                  var phase = (float)rows.GetValue(3);
                  var location = Encoding.UTF8.GetString((byte[])rows.GetValue(4));
                  Console.WriteLine(
                        $"ts: {ts:yyyy-MM-dd HH:mm:ss.fff}, current: {current}, voltage: {voltage}, phase: {phase}, location: {location}");
               }
            }
        }
    }
}
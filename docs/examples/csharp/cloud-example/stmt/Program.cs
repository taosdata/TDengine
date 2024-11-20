using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Cloud.Examples
{
    public class STMTExample
    {
        static void Main(string[] args)
        {
            var numOfSubTable = 10;
            var numOfRow = 10;
            var random = new Random();
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
                  // use database
                  client.Exec("USE test");
                  // assume table has been created.
                  using (var stmt = client.StmtInit())
                  {
                     String sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
                     stmt.Prepare(sql);
                     for (int i = 1; i <= numOfSubTable; i++)
                     {
                        var tableName = $"d_bind_{i}";
                        // set table name
                        stmt.SetTableName(tableName);
                        // set tags
                        stmt.SetTags(new object[] { i, $"location_{i}" });
                        var current = DateTime.Now;
                        // bind rows
                        for (int j = 0; j < numOfRow; j++)
                        {
                           stmt.BindRow(new object[]
                           {
                              current.Add(TimeSpan.FromMilliseconds(j)),
                              random.NextSingle() * 30,
                              random.Next(300),
                              random.NextSingle()
                           });
                        }
                        // add batch
                        stmt.AddBatch();
                        // execute
                        stmt.Exec();
                        // get affected rows
                        var affectedRows = stmt.Affected();
                        Console.WriteLine($"Successfully inserted {affectedRows} rows to {tableName}.");
                     }
                  }
               }
            }
            catch (TDengineError e)
            {
               // handle TDengine error
               Console.WriteLine("Failed to insert to table meters using stmt, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
               throw;
            }
            catch (Exception e)
            {
               // handle other exceptions
               Console.WriteLine("Failed to insert to table meters using stmt, ErrMessage: " + e.Message);
               throw;
            }
        }
    }
}
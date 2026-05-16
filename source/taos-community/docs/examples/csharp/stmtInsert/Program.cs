using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class StmtInsertExample
    {
        // ANCHOR: main
        public static void Main(string[] args)
        {
            var host = "127.0.0.1";
            var numOfSubTable = 10;
            var numOfRow = 10;
            var random = new Random();
            var connectionString = $"host={host};port=6030;username=root;password=taosdata";
            try
            {
                var builder = new ConnectionStringBuilder(connectionString);
                using (var client = DbDriver.Open(builder))
                {
                    // create database
                    client.Exec("CREATE DATABASE IF NOT EXISTS power");
                    // use database
                    client.Exec("USE power");
                    // create table
                    client.Exec(
                        "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
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
        // ANCHOR_END: main
    }
}

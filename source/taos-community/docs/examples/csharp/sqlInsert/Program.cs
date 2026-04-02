using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class SQLInsertExample
    {
        public static void Main(string[] args)
        {
            try
            {
                
                var builder = new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
                using (var client = DbDriver.Open(builder))
                {
                    CreateDatabaseAndTable(client);
                    InsertData(client);
                    QueryData(client);
                    QueryWithReqId(client);
                }
            }
            catch (TDengineError e)
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

        private static void CreateDatabaseAndTable(ITDengineClient client)
        {
            // ANCHOR: create_db_and_table
            try
            {
                // create database
                var affected = client.Exec("CREATE DATABASE IF NOT EXISTS power");
                Console.WriteLine($"Create database power, affected rows: {affected}");
                // create table
                affected = client.Exec(
                    "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                Console.WriteLine($"Create table meters, affected rows: {affected}");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to create db and table; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to create db and table; Err:" + e.Message);
                throw;
            }
            // ANCHOR_END: create_db_and_table
        }

        private static void InsertData(ITDengineClient client)
        {
            // ANCHOR: insert_data
            try
            {
                // insert data
                var insertQuery = "INSERT INTO " +
                                  "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                                  "VALUES " +
                                  "(NOW + 1a, 10.30000, 219, 0.31000) " +
                                  "(NOW + 2a, 12.60000, 218, 0.33000) " +
                                  "(NOW + 3a, 12.30000, 221, 0.31000) " +
                                  "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                                  "VALUES " +
                                  "(NOW + 1a, 10.30000, 218, 0.25000) ";
                var affectedRows = client.Exec(insertQuery);
                Console.WriteLine("insert " + affectedRows + " rows to power.meters successfully.");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to insert data to power.meters; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to insert data to power.meters; Err:" + e.Message);
                throw;
            }
            // ANCHOR_END: insert_data
        }

        private static void QueryData(ITDengineClient client)
        {
            // ANCHOR: select_data
            try
            {
                // query data
                var query = "SELECT ts, current, location FROM power.meters limit 100";
                using (var rows = client.Query(query))
                {
                    while (rows.Read())
                    {
                        var ts = (DateTime)rows.GetValue(0);
                        var current = (float)rows.GetValue(1);
                        var location = Encoding.UTF8.GetString((byte[])rows.GetValue(2));
                        Console.WriteLine(
                            $"ts: {ts:yyyy-MM-dd HH:mm:ss.fff}, current: {current}, location: {location}");
                    }
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to query data from power.meters; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to query data from power.meters; Err:" + e.Message);
                throw;
            }
            // ANCHOR_END: select_data
        }

        private static void QueryWithReqId(ITDengineClient client)
        {
            // ANCHOR: query_id
            try
            {
                // query data
                var query = "SELECT ts, current, location FROM power.meters limit 1";
                // query with request id 3
                using (var rows = client.Query(query,3))
                {
                    while (rows.Read())
                    {
                        var ts = (DateTime)rows.GetValue(0);
                        var current = (float)rows.GetValue(1);
                        var location = Encoding.UTF8.GetString((byte[])rows.GetValue(2));
                        Console.WriteLine(
                            $"ts: {ts:yyyy-MM-dd HH:mm:ss.fff}, current: {current}, location: {location}");
                    }
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to execute sql with reqId; ErrCode:" + e.Code + "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to execute sql with reqId; Err:" + e.Message);
                throw;
            }
            // ANCHOR_END: query_id
        }
    }
}
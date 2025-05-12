using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Examples
{
    public class WSInsertExample
    {
        public static void Main(string[] args)
        {
            try
            {
                var connectionString =
                    "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false;username=root;password=taosdata";
                var builder = new ConnectionStringBuilder(connectionString);
                using (var client = DbDriver.Open(builder))
                {
                    CreateDatabaseAndTable(client, connectionString);
                    InsertData(client, connectionString);
                    QueryData(client, connectionString);
                    QueryWithReqId(client, connectionString);
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

        private static void CreateDatabaseAndTable(ITDengineClient client, string connectionString)
        {
            // ANCHOR: create_db_and_table
            try
            {
                // create database
                var affected = client.Exec("CREATE DATABASE IF NOT EXISTS power");
                Console.WriteLine($"Create database power successfully, rowsAffected: {affected}");
                // create table
                affected = client.Exec(
                    "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                Console.WriteLine($"Create stable power.meters successfully, rowsAffected: {affected}");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to create database power or stable meters, ErrCode: " + e.Code +
                                  ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to create database power or stable meters, ErrMessage: " + e.Message);
                throw;
            }
            // ANCHOR_END: create_db_and_table
        }

        private static void InsertData(ITDengineClient client, string connectionString)
        {
            // ANCHOR: insert_data
            // insert data, please make sure the database and table are created before
            var insertQuery = "INSERT INTO " +
                              "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                              "VALUES " +
                              "(NOW + 1a, 10.30000, 219, 0.31000) " +
                              "(NOW + 2a, 12.60000, 218, 0.33000) " +
                              "(NOW + 3a, 12.30000, 221, 0.31000) " +
                              "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                              "VALUES " +
                              "(NOW + 1a, 10.30000, 218, 0.25000) ";
            try
            {
                var affectedRows = client.Exec(insertQuery);
                Console.WriteLine("Successfully inserted " + affectedRows + " rows to power.meters.");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to insert data to power.meters, sql: " + insertQuery + ", ErrCode: " +
                                  e.Code + ", ErrMessage: " +
                                  e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to insert data to power.meters, sql: " + insertQuery + ", ErrMessage: " +
                                  e.Message);
                throw;
            }
            // ANCHOR_END: insert_data
        }

        private static void QueryData(ITDengineClient client, string connectionString)
        {
            // ANCHOR: select_data
            // query data, make sure the database and table are created before
            var query = "SELECT ts, current, location FROM power.meters limit 100";
            try
            {
                using (var rows = client.Query(query))
                {
                    while (rows.Read())
                    {
                        // Add your data processing logic here
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
                Console.WriteLine("Failed to query data from power.meters, sql: " + query + ", ErrCode: " + e.Code +
                                  ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    "Failed to query data from power.meters, sql: " + query + ", ErrMessage: " + e.Message);
                throw;
            }
            // ANCHOR_END: select_data
        }

        private static void QueryWithReqId(ITDengineClient client, string connectionString)
        {
            // ANCHOR: query_id
            var reqId = (long)3;
            // query data
            var query = "SELECT ts, current, location FROM power.meters limit 1";
            try
            {
                // query with request id 3
                using (var rows = client.Query(query, reqId))
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
                Console.WriteLine("Failed to execute sql with reqId: " + reqId + ", sql: " + query + ", ErrCode: " +
                                  e.Code + ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to execute sql with reqId: " + reqId + ", sql: " + query + ", ErrMessage: " +
                                  e.Message);
                throw;
            }
            // ANCHOR_END: query_id
        }
    }
}
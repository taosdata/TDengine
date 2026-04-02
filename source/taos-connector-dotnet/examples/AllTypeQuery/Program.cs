using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace AllTypeQuery
{
    public class Example
    {
        public static void Main(string[] args)
        {
            try
            {
                // ws connection string
                var connectionString = "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false;username=root;password=taosdata";

                // native connection string
                // var connectionString = "protocol=Native;host=127.0.0.1;port=6030;username=root;password=taosdata";

                var builder = new ConnectionStringBuilder(connectionString);
                using (var client = DbDriver.Open(builder))
                {
                    // create database
                    var affected = client.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_query");
                    Console.WriteLine($"Create database example_all_type_query successfully, rowsAffected: {affected}");
                    
                    // create table with json
                    affected = client.Exec(
                        "CREATE STABLE IF NOT EXISTS example_all_type_query.stb_json (" +
                        "ts TIMESTAMP, " +
                        "int_col INT) " +
                        "tags (json_tag json)");
                    Console.WriteLine(
                        $"Create stable example_all_type_query.stb_json successfully, rowsAffected: {affected}");
                    var insertQuery =
                        "INSERT INTO example_all_type_query.ntb_json using example_all_type_query.stb_json tags('{\"device\":\"device_1\"}') " +
                        "values(now, 1)";
                    var affectedRows = client.Exec(insertQuery);
                    Console.WriteLine("Successfully inserted " + affectedRows +
                                      " rows to example_all_type_query.ntb_json.");
                    var query = "SELECT * FROM example_all_type_query.stb_json";

                    using (var rows = client.Query(query))
                    {
                        while (rows.Read())
                        {
                            // Add your data processing logic here
                            var ts = (DateTime)rows.GetValue(0);
                            var intVal = (int)rows.GetValue(1);
                            var jsonVal = (byte[])rows.GetValue(2);
                            Console.WriteLine(
                                $"ts: {ts:yyyy-MM-dd HH:mm:ss.fff}, " +
                                $"int_col: {intVal}, " +
                                $"json_tag: {Encoding.UTF8.GetString(jsonVal)}"
                            );
                        }
                    }
                    
                    // create table without json
                    affected = client.Exec(
                        "CREATE STABLE IF NOT EXISTS example_all_type_query.stb (" +
                        "ts TIMESTAMP, " +
                        "int_col INT, " +
                        "double_col DOUBLE, " +
                        "bool_col BOOL, " +
                        "binary_col BINARY(100), " +
                        "nchar_col NCHAR(100), " +
                        "varbinary_col VARBINARY(100), " +
                        "geometry_col GEOMETRY(100)) " +
                        "tags (" +
                        "int_tag INT, " +
                        "double_tag DOUBLE, " +
                        "bool_tag BOOL, " +
                        "binary_tag BINARY(100), " +
                        "nchar_tag NCHAR(100), " +
                        "varbinary_tag VARBINARY(100), " +
                        "geometry_tag GEOMETRY(100)) ");
                    Console.WriteLine(
                        $"Create stable example_all_type_query.stb successfully, rowsAffected: {affected}");
                    insertQuery =
                        "INSERT INTO example_all_type_query.ntb using example_all_type_query.stb tags(1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)') " +
                        "values(now, 1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)')";
                    affectedRows = client.Exec(insertQuery);
                    Console.WriteLine("Successfully inserted " + affectedRows +
                                      " rows to example_all_type_query.ntb.");
                    query = "SELECT * FROM example_all_type_query.stb";

                    using (var rows = client.Query(query))
                    {
                        while (rows.Read())
                        {
                            // Add your data processing logic here
                            var ts = (DateTime)rows.GetValue(0);
                            var intVal = (int)rows.GetValue(1);
                            var doubleVal = (double)rows.GetValue(2);
                            var boolVal = (bool)rows.GetValue(3);
                            var binaryVal = (byte[])rows.GetValue(4);
                            var ncharVal = (string)rows.GetValue(5);
                            var varbinaryVal = (byte[])rows.GetValue(6);
                            var geometryVal = (byte[])rows.GetValue(7);
                            var intTag = (int)rows.GetValue(8);
                            var doubleTag = (double)rows.GetValue(9);
                            var boolTag = (bool)rows.GetValue(10);
                            var binaryTag = (byte[])rows.GetValue(11);
                            var ncharTag = (string)rows.GetValue(12);
                            var varbinaryTag = (byte[])rows.GetValue(13);
                            var geometryTag = (byte[])rows.GetValue(14);

                            Console.WriteLine(
                                $"ts: {ts:yyyy-MM-dd HH:mm:ss.fff}, " +
                                $"int_col: {intVal}, " +
                                $"double_col: {doubleVal}, " +
                                $"bool_col: {boolVal}, " +
                                $"binary_col: {Encoding.UTF8.GetString(binaryVal)}, " +
                                $"nchar_col: {ncharVal}, " +
                                $"varbinary_col: {BitConverter.ToString(varbinaryVal)}, " +
                                $"geometry_col: {BitConverter.ToString(geometryVal)}, " +
                                $"int_tag: {intTag}, " +
                                $"double_tag: {doubleTag}, " +
                                $"bool_tag: {boolTag}, " +
                                $"binary_tag: {Encoding.UTF8.GetString(binaryTag)}, " +
                                $"nchar_tag: {ncharTag}, " +
                                $"varbinary_tag: {BitConverter.ToString(varbinaryTag)}, " +
                                $"geometry_tag: {BitConverter.ToString(geometryTag)}"
                            );
                        }
                    }
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
    }
}
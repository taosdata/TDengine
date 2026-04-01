using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace AllTypeStmt
{
    internal class Example
    {
        public static void Main(string[] args)
        {
            // ws connection string
            var connectionString = "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false;username=root;password=taosdata";

            // natives connection string
            // var connectionString = "protocol=Native;host=127.0.0.1;port=6030;username=root;password=taosdata";

            try
            {
                var builder = new ConnectionStringBuilder(connectionString);
                using (var client = DbDriver.Open(builder))
                {
                    // create database
                    client.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_stmt");
                    // use database
                    client.Exec("USE example_all_type_stmt");
                    // create table with json
                    client.Exec(
                        "CREATE STABLE IF NOT EXISTS stb_json (" +
                        "ts TIMESTAMP, " +
                        "int_col INT) " +
                        "tags (json_tag json)");
                    // craete table without json
                    client.Exec(
                        "CREATE STABLE IF NOT EXISTS stb (" +
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
                        "geometry_tag GEOMETRY(100))");
                    using (var stmt = client.StmtInit())
                    {
                        // prepare sql with json
                        String sql = "INSERT INTO ? using stb_json tags(?) VALUES (?,?)";
                        stmt.Prepare(sql);
                        // set table name
                        stmt.SetTableName("ntb_json");
                        // set tags
                        stmt.SetTags(new object[]
                        {
                            Encoding.UTF8.GetBytes("{\"device\":\"device_1\"}")
                        });
                        
                        var current = DateTime.Now;
                        // bind rows
                        stmt.BindRow(new object[]
                        {
                            // ts
                            current,
                            // int_col
                            (int)1,
                        });
                        
                        // add batch
                        stmt.AddBatch();
                        // execute
                        stmt.Exec();
                        // get affected rows
                        var affectedRows = stmt.Affected();
                        Console.WriteLine($"Successfully inserted {affectedRows} rows to example_all_type_stmt.ntb_json");
                        
                        // prepare sql without json
                        sql = "INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)";
                        stmt.Prepare(sql);
                        // set table name
                        stmt.SetTableName("ntb");
                        // set tags
                        stmt.SetTags(new object[]
                        {
                            // int_tag
                            (int)1,
                            // double_tag
                            (double) 1.1,
                            // bool_tag
                            (bool) true,
                            // binary_tag
                            Encoding.UTF8.GetBytes("binary_value"),
                            // nchar_tag
                            "nchar_value",
                            // varbinary_tag
                            new byte[]
                            {
                                0x98,0xf4,0x6e
                            },
                            // geometry_tag
                            new byte[]
                            {
                                0x01, 0x01, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x59,
                                0x40, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x59, 0x40
                            },
                        });

                        stmt.BindRow(new object[]
                        {
                            // ts
                            current,
                            // int_col
                            (int)1,
                            // double_col
                            (double) 1.1,
                            // bool_col
                            (bool) true,
                            // binary_col
                            Encoding.UTF8.GetBytes("binary_value"),
                            // nchar_col
                            "nchar_value",
                            // varbinary_col
                            new byte[]
                            {
                                0x98,0xf4,0x6e
                            },
                            // geometry_col
                            new byte[]
                            {
                                0x01, 0x01, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x59,
                                0x40, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x59, 0x40
                            },
                        });

                        // add batch
                        stmt.AddBatch();
                        // execute
                        stmt.Exec();
                        // get affected rows
                        affectedRows = stmt.Affected();
                        Console.WriteLine($"Successfully inserted {affectedRows} rows to example_all_type_stmt.ntb");
                    }
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to insert to table meters using stmt, ErrCode: " + e.Code + ", ErrMessage: " +
                                  e.Error);
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
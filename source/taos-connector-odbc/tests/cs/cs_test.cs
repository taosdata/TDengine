/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using System.Text;
using System.Data.Odbc;
using System.Runtime.CompilerServices;
using System.Data;
using System;

namespace ConsoleApp1
{
    internal class Program
    {
        private static void execute_non_query(string connString, string sqls)
        {
            OdbcCommand command = new OdbcCommand(sqls);

            using (OdbcConnection connection = new OdbcConnection(connString))
            {
                command.Connection = connection;
                connection.Open();
                command.ExecuteNonQuery();

                // The connection is automatically closed at
                // the end of the Using block.
            }
        }
        private static void execute_query(string connectionString, string queryString)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                OdbcCommand command = new OdbcCommand(queryString, connection);

                connection.Open();

                // Execute the DataReader and access the data.
                OdbcDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.WriteLine("=={0}:{1}", i, reader[i]);
                    }
                }

                // Call Close when done reading.
                reader.Close();
            }
        }
        private static void execute_query_with_params(string connectionString, string cmdString, string[] values)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                // Create the command and set its properties.
                OdbcCommand command = new OdbcCommand();
                command.Connection = connection;
                command.CommandText = cmdString;
                command.CommandType = CommandType.Text;

                foreach (string value in values)
                {
                    OdbcParameter parameter = new OdbcParameter();
                    parameter.OdbcType = OdbcType.VarChar;
                    parameter.Direction = ParameterDirection.Input;
                    parameter.Value = value;

                    // Add the parameter to the Parameters collection.
                    command.Parameters.Add(parameter);
                }

                // Open the connection and execute the reader.
                connection.Open();
                using (OdbcDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.WriteLine("=={0}:{1}", i, reader[i]);
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("No rows found.");
                    }
                    reader.Close();
                }
            }
        }
        private static void run(bool ws, string ts)
        {
            string connString  = "DSN=TAOS_ODBC_DSN";
            if (ws) connString = "DSN=TAOS_ODBC_WS_DSN";

            string queryString = "drop database if exists foo; create database if not exists foo; use foo; drop table if exists t;create table t(ts timestamp, name varchar(20), mark nchar(20)); insert into t (ts, name, mark) values (now(), '测试', '人物')";
            execute_non_query(connString, queryString);
            execute_query(connString, "select * from foo.t");

            Console.WriteLine(ts);
            execute_query_with_params(connString, "insert into foo.t (ts, name, mark) values (?, ?, ?)", new string [] {ts, "测试", "人物"});

            if (ws) {
                // NOTE: does taosws-rs support select-with-params?
                return;
            }
            execute_query_with_params(connString, "select * from t where name = ?", new string [] {"测试"});
            execute_query_with_params(connString, "select * from t where mark = ?", new string [] {"人物"});
            execute_query_with_params(connString, "select * from t where mark = ?", new string [] {"人物z"});
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");
            // string connString;
            // string queryString;
            // if (false) {
            //     connString = "DSN=SQLSERVER_ODBC_DSN";
            //     queryString = "drop table if exists t;create table t(name varchar(20), mark nchar(20)); insert into t (name, mark) values ('测试', '人物')";
            //     execute_non_query(connString, queryString);
            //     execute_query(connString, "select * from t");
            //     execute_query_with_params(connString, "insert into t (name, mark) values (?, ?)", new string [] {"测试", "人物"});
            //     execute_query_with_params(connString, "select * from t where name = ?", new string [] {"测试"});
            //     execute_query_with_params(connString, "select * from t where mark = ?", new string [] {"人物"});
            // }

            String ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            String tsZ = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
#if HAVE_TAOS
            run(false, ts);
            run(false, tsZ);
#endif
#if HAVE_TAOSWS
            run(true, ts);
            run(true, tsZ);
#endif
            Console.WriteLine("success!");
        }
    }
}
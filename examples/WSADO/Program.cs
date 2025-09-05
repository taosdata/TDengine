using System;
using TDengine.Data.Client;

namespace WSADO
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            const string connectionString = "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata";
            using (var connection = new TDengineConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    using (var command = new TDengineCommand(connection))
                    {
                        command.CommandText = "create database power";
                        command.ExecuteNonQuery();
                        connection.ChangeDatabase("power");
                        command.CommandText =
                            "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))";
                        command.ExecuteNonQuery();
                        command.CommandText = "INSERT INTO " +
                                              "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                                              "VALUES " +
                                              "(?,?,?,?)";
                        var parameters = command.Parameters;
                        parameters.Add(new TDengineParameter("@0", DateTime.Now));
                        parameters.Add(new TDengineParameter("@1", (float)10.30000));
                        parameters.Add(new TDengineParameter("@2", (int)219));
                        parameters.Add(new TDengineParameter("@3", (float)0.31000));
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                        command.CommandText = "SELECT * FROM meters";
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine(
                                    $"{((DateTime) reader.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {reader.GetValue(1)}, {reader.GetValue(2)}, {reader.GetValue(3)}, {reader.GetValue(4)}, {System.Text.Encoding.UTF8.GetString((byte[]) reader.GetValue(5))}");
                            }
                        }
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
using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Examples
{
    public class WSQueryExample
    {
        public static void Main(string[] args)
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec("use power");
                    string query = "SELECT * FROM meters";
                    using (var rows = client.Query(query))
                    {
                        while (rows.Read())
                        {
                            Console.WriteLine(
                                $"{((DateTime)rows.GetValue(0)):yyyy-MM-dd HH:mm:ss.fff}, {rows.GetValue(1)}, {rows.GetValue(2)}, {rows.GetValue(3)}, {rows.GetValue(4)}, {Encoding.UTF8.GetString((byte[])rows.GetValue(5))}");
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    throw;
                }
            }
        }
    }
}
using System;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Examples
{
    public class WSConnExample
    {
        static void Main(string[] args)
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                Console.WriteLine("connected");
            }
        }
    }
}

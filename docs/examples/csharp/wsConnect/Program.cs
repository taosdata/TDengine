using System;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Examples
{
    public class WSConnExample
    {
        // ANCHOR: main
        static void Main(string[] args)
        {
            try
            {
                // Connect to TDengine server using WebSocket
                var builder = new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
                // Open connection with using block, it will close the connection automatically
                using (var client = DbDriver.Open(builder))
                {
                    Console.WriteLine("connected");
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
        // ANCHOR_END: main
    }
}
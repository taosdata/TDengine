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
            var connectionString =
                "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata";
            try
            {
                // Connect to TDengine server using WebSocket
                var builder = new ConnectionStringBuilder(connectionString);
                // Open connection with using block, it will close the connection automatically
                using (var client = DbDriver.Open(builder))
                {
                    Console.WriteLine("Connected to " + connectionString + " successfully.");
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to connect to " + connectionString + "; ErrCode:" + e.Code +
                                  "; ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to connect to " + connectionString + "; Err:" + e.Message);
                throw;
            }
        }
        // ANCHOR_END: main
    }
}
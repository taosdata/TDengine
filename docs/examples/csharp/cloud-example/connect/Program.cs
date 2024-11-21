using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Cloud.Examples
{
    public class ConnectExample
    {
        static void Main(string[] args)
        {
           var cloudEndPoint = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
           var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
           var connectionString = $"protocol=WebSocket;host={cloudEndPoint};port=443;useSSL=true;token={cloudToken};";
           // Connect to TDengine server using WebSocket
           var builder = new ConnectionStringBuilder(connectionString);
          try
          {
             // Open connection with using block, it will close the connection automatically
             using (var client = DbDriver.Open(builder))
             {
               Console.WriteLine("Connected to " + builder.ToString() + " successfully.");
             }
          }
          catch (TDengineError e)
          {
             // handle TDengine error
             Console.WriteLine("Failed to connect to " + builder.ToString() + "; ErrCode:" + e.Code +
                                              "; ErrMessage: " + e.Error);
             throw;
          }
          catch (Exception e)
          {
             // handle other exceptions
             Console.WriteLine("Failed to connect to " + builder.ToString() + "; Err:" + e.Message);
             throw;
          }
        }
    }
}
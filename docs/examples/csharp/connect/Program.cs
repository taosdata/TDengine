using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{
    internal class ConnectExample
    {
        // ANCHOR: main
        static void Main(String[] args)
        {
            try
            {
                // Connect to TDengine server using Native
                var builder = new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
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
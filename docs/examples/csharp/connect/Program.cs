using TDengine.Driver;
using TDengine.Driver.Client;

namespace TDengineExample
{

    internal class ConnectExample
    {
        static void Main(String[] args)
        {
            var builder = new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                Console.WriteLine("connected");
            }
        }
    }
}

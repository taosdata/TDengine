using TDengineDriver;

namespace TDengineExample
{

    internal class ConnectExample
    {
        static void Main(String[] args)
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";

            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                Console.WriteLine("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            TDengine.Close(conn);
            TDengine.Cleanup();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TDEngineConnectionTest
{
    class Program
    {
        static void Main(string[] args)
        {
            List<string> tags = new List<string>() { "1sinusoid!", "cdt158", "sinusoid", "sinusoidu", "TEST_DIGITAL", "TEST_FLOAT16", "TEST_FLOAT32", "TEST_FLOAT64", "TEST_INT16", "TEST_INT32", "TEST_STRING", "TEST_TIMESTAMP" };

            AppService appService = new AppService();
            Task.Run(async () =>
            {
                await appService.Connect();
                await appService.MigratePIPoints(tags);
                await appService.MigrateAFDatabase("Weather");

            }).Wait();
            Console.WriteLine("Finished");
            Console.ReadKey();


            ////example of subscibing to PI Points and writing to TDengine

            //List<string> piPoints = new List<string>() { "cdt158", "sinusoid" };
            //var piServer = "tdengine-tde-pi.westus.cloudapp.azure.com";
            //var piSystem = "TDE-PI";

            //AppService appService = new AppService(piServer, piSystem, "172.30.160.1", 6041, "root", "taosdata", piPoints);

            //// Create the thread object, passing in the
            //// serverObject.InstanceMethod method using a
            //// ThreadStart delegate.
            //Thread connector = new Thread(
            //    new ThreadStart(appService.Subscribe));

            //// Start the thread.
            //connector.Start();
            //Console.WriteLine("Finished");
            //Console.ReadKey();

        }
    }
}

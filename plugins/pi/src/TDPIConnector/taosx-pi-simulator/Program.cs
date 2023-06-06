using log4net;
using log4net.Config;
using System;
using TDPIConnector.Core;
using System.Threading;
using System.Reflection;
using System.Linq;
using System.Diagnostics;

namespace PISimulator
{
    internal class Program
    {
        private static bool logInit = LogInit();
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static bool LogInit()
        {
            GlobalContext.Properties["applicationName"] = "simulator";
            GlobalContext.Properties["pid"] = Process.GetCurrentProcess().Id;
            XmlConfigurator.Configure(new System.IO.FileInfo("log4net.config"));
            return true;
        }
        static void PrintVersion(bool writelog)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            var cus_ttributes = assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>();
            var build_time = cus_ttributes.FirstOrDefault(a => a.Key == "BuildTime").Value;
            var commit = cus_ttributes.FirstOrDefault(a => a.Key == "Commit").Value;

            AssemblyName assemblyName = assembly.GetName();
            Version version = assemblyName.Version;

            if (writelog)
            {
                log.Info("PI Simulator version is: " + version);
                log.Info("PI Simulator commit is: " + commit);
                log.Info("PI BaSimulatorckfill build at: " + build_time);
            }
            else {
                Console.WriteLine("PI Simulator");
                Console.WriteLine($"    Version : {version}");
                Console.WriteLine($"    Commit : {commit}");
                Console.WriteLine($"    Build Time : {build_time}");
            }
        }
        static void Main(string[] args)
        {
            //create a command line parser using args
            CommandLineParser parser = new CommandLineParser(args);
            //get the command line options
            CommandLineOptions options = parser.GetCommandLineOptions();
            if (options.ShowVersion) {
                PrintVersion(false);
                return;
            }
            PrintVersion(true);
            if (options.Help)
            {
                //output to console the help message
                Console.WriteLine("Help Message:");
                Console.WriteLine("Usage: TDBackfill.exe [options]");
                Console.WriteLine("Options:");
                Console.WriteLine("-h, --help");
                Console.WriteLine("    Display this help message.");
                Console.WriteLine("-v, --version");
                Console.WriteLine("    Display version Information.");
            }
            else
            {
                try {
                    AppSettings.Init(options.tomlFile);
                }
                catch (Exception e) {
                    log.Fatal("Init Failed! Please check toml config file.", e);
                    return;
                }
          
                var simlulator = new SimulatorFromCSV(AppSettings.tomlConfig.PIServerName);
                simlulator.Start();

                
            }

            while (true)
            {
                var str = Console.ReadLine();
                if (str == "quit")
                {
                    log.Info("PI Simulator Connector quit...");
                    break;
                }
                else
                {
                    Thread.Sleep(5000);
                }
            }
            log.Info("PI Simulator finished, exit.");
        }
    }
}

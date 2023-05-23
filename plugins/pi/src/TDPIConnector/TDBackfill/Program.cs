using log4net;
using log4net.Config;
using System;
using TDPIConnector.Core;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using System.Reflection;
using System.Linq;

namespace TDBackfill
{
    internal class Program
    {
        private static bool logInit = LogInit();
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static bool LogInit()
        {
            GlobalContext.Properties["applicationName"] = "backfill";
            XmlConfigurator.Configure(new System.IO.FileInfo("log4net.config"));
            return true;
        }
        static void PrintVersion()
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            var cus_ttributes = assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>();
            var build_time = cus_ttributes.FirstOrDefault(a => a.Key == "BuildTime").Value;
            var commit = cus_ttributes.FirstOrDefault(a => a.Key == "Commit").Value;

            AssemblyName assemblyName = assembly.GetName();
            Version version = assemblyName.Version;

            log.Info("PI Backfill version is: " + version);
            log.Info("PI Backfill commit is: " + commit);
            log.Info("PI Backfill build at: " + build_time);
            Console.WriteLine("PI Backfill{}");
            Console.WriteLine($"    Version : {version}");
            Console.WriteLine($"    Commit : {commit}");
            Console.WriteLine($"    Build Time : {build_time}");
        }
        static void Main(string[] args)
        {
            PrintVersion();
            //create a command line parser using args
            CommandLineParser parser = new CommandLineParser(args);
            //get the command line options
            CommandLineOptions options = parser.GetCommandLineOptions();

            if (options.Help)
            {
                //output to console the help message
                Console.WriteLine("Help Message:");
                Console.WriteLine("Usage: TDBackfill.exe [options]");
                Console.WriteLine("Options:");
                Console.WriteLine("-h, --help");
                Console.WriteLine("    Display this help message.");
                Console.WriteLine("-drop, --drop-table");
                Console.WriteLine("    Drop the associated table before backfilling.");
                Console.WriteLine("    This will delete all data in the table. Ignored if -t or -f are specified.");
                Console.WriteLine("-a, --all");
                Console.WriteLine("    Backfill all data.");
                Console.WriteLine("-to, --to-first-recorded");
                Console.WriteLine("    Backfill up to the first recorded value in TDengine.");
                Console.WriteLine("-from, --from-last-recorded");
                Console.WriteLine("    Backfill from the last recorded value in TDengine.");
                Console.WriteLine("-f, --file-toml");
                Console.WriteLine("    Backfill toml config path.");              
                Console.WriteLine("-s, --start");
                Console.WriteLine("    The start time for backfilling data. If not provided, the start time will be the earliest time available.");
                Console.WriteLine("-e, --end");
                Console.WriteLine("    The end time for backfilling data. If not provided, the end time will be the current time.");
                Console.WriteLine("Examples:");
                Console.WriteLine("    TDBackfill.exe -drop -a");
                Console.WriteLine("    TDBackfill.exe -t");
                Console.WriteLine("    TDBackfill.exe -f");
                Console.WriteLine("    TDBackfill.exe -drop -s 1/1/2014 -e 1/2/2014");
                Console.WriteLine("    TDBackfill.exe -t -s \"1/1/2014 10 am\"");
                Console.WriteLine("    TDBackfill.exe -f -e \"1/2/2014 14:32:12\"");
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

                if (options.tomlFile == "") {
                    AppSettings.tomlConfig.SetBackfillOption(
                        options.BackfillToFirstRecorded,
                        options.BackfillFromLastRecorded,
                        options.Start,
                        options.End
                        );
                }

                PISystemManager piSystemManager = null;
                if (!string.IsNullOrEmpty(AppSettings.tomlConfig.PISystemName)) {
                    piSystemManager = new PISystemManager(AppSettings.tomlConfig.PISystemName);
                }
                PIServerManager piServerManager = null;
                if (!string.IsNullOrEmpty(AppSettings.tomlConfig.PIServerName))
                {
                    piServerManager = new PIServerManager(AppSettings.tomlConfig.PIServerName);
                }

                TDEngineProxy tdEngineProxy;
                if (!AppSettings.TaosXEnabled)
                {
                    tdEngineProxy = TDEngineProxyBuild.NewTDEngineClient(AppSettings.TDEngineHost,
                        AppSettings.TDEnginePort,
                        AppSettings.TDEngineUsername,
                        AppSettings.TDEnginePassword,
                        AppSettings.TDEngineToken,
                        AppSettings.TDEnginePITablesPrefix
                        );
                }
                else
                {
                    tdEngineProxy = TDEngineProxyBuild.NewTDEngineProxy(AppSettings.tomlConfig.IPCStream,
                        AppSettings.tomlConfig.SQLAPI,
                        AppSettings.TDEnginePITablesPrefix,
                        AppSettings.tomlConfig.MaxWaitLen
                        );
                }

                try
                {
                    tdEngineProxy.VerifyLicenseCompability();
                }
                catch (Exception e)
                {
                    log.Fatal("Error starting the application.", e);
                    throw e;
                }

                try

                {
                    if (piServerManager != null)
                    {
                        piServerManager.Connect();
                    }
                    if (piSystemManager != null)
                    {
                        piSystemManager.Connect();
                    }
                    tdEngineProxy.Connect();
                }
                catch (Exception e)
                {
                    log.Fatal("Error starting the application.", e);
                    throw e;
                }

                BackfillManager backfillManager = new BackfillManager(piSystemManager, piServerManager, tdEngineProxy);

                try
                {
                    backfillManager.BackfillPIPointsFromTool(AppSettings.tomlConfig.TDDataBase,
                        AppSettings.tomlConfig.AFDatabaseName,
                        AppSettings.tomlConfig.BackfillStartTime,
                        AppSettings.tomlConfig.BackfillEndTime,
                        AppSettings.tomlConfig.ToTDengineFirstTime,
                        AppSettings.tomlConfig.FromTDengineLastTime,
                        options.DropTables).Wait();
                }
                catch (Exception e)
                {
                    log.Error("Error backfilling PI Points", e.InnerException);
                }

                try
                {
                    if (piSystemManager != null)
                    {
                        backfillManager.BackfillAFElementsFromTool(AppSettings.tomlConfig.TDDataBase,
                                AppSettings.tomlConfig.AFDatabaseName,
                                AppSettings.tomlConfig.TemplateForAFElement,
                                AppSettings.tomlConfig.BackfillStartTime,
                                AppSettings.tomlConfig.BackfillEndTime,
                                AppSettings.tomlConfig.ToTDengineFirstTime,
                                AppSettings.tomlConfig.FromTDengineLastTime,
                                options.DropTables).Wait();
                    }

                }
                catch (Exception e)
                {
                    log.Error("Error backfilling AF Elements", e.InnerException);
                }

                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }
    }
}

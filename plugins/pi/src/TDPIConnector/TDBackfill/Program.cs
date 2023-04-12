using log4net;
using System;
using TDPIConnector.Core;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;

namespace TDBackfill
{
    internal class Program
    {
        //private static readonly ILog logger = LogManager.GetLogger(typeof(Program));
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        static void Main(string[] args)
        {
            log.Info("test");

            //create a command line parser using args
            CommandLineParser parser = new CommandLineParser(args);
            //get the command line options
            CommandLineOptions options = parser.GetCommandLineOptions();

            //output to console command line options
            //Console.WriteLine("Help: " + options.Help.ToString());
            //Console.WriteLine("Drop Table: " + options.DropTable.ToString());
            //Console.WriteLine("Backfill All: " + options.BackfillAll.ToString());
            //Console.WriteLine("Backfill To Last Recorded: " + options.BackfillToLastRecorded.ToString());
            //Console.WriteLine("Backfill From Last Recorded: " + options.BackfillFromLastRecorded.ToString());
            //Console.WriteLine("Start: " + options.Start.ToString());
            //Console.WriteLine("End: " + options.End.ToString());

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
                Console.WriteLine("-t, --to-first-recorded");
                Console.WriteLine("    Backfill up to the first recorded value in TDengine.");
                Console.WriteLine("-f, --from-last-recorded");
                Console.WriteLine("    Backfill from the last recorded value in TDengine.");
                Console.WriteLine("-file, --file-toml");
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
                AppSettings.Init(options.tomlFile);

                var piSystemManager = new PISystemManager(AppSettings.tomlConfig.PISystemName);
                var piServerManager = new PIServerManager(AppSettings.tomlConfig.PIServerName);
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
                    piServerManager.Connect();
                    piSystemManager.Connect();
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

                    backfillManager.BackfillPIPointsFromTool(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName, options.Start, options.End,
                         options.BackfillToFirstRecorded, options.BackfillFromLastRecorded,
                         options.DropTables).Wait();
                }
                catch (Exception e)
                {
                    log.Error("Error backfilling PI Points", e.InnerException);
                }

                try
                {

                    backfillManager.BackfillAFElementsFromTool(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.TemplateForAFElement, options.Start, options.End,
                                options.BackfillToFirstRecorded, options.BackfillFromLastRecorded,
                                options.DropTables).Wait();

                }
                catch (Exception e)
                {
                    log.Error("Error backfilling AF Elements", e.InnerException);
                }


                log.Info("test");

                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }
    }
}

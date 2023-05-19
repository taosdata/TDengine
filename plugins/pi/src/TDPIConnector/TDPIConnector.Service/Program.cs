using log4net;
using log4net.Config;
using System;
using System.ServiceProcess;
using TDPIConnector.Core;
using System.Threading;

namespace TDPIConnector.Service
{
    static class Program
    {
        enum WorkMode {
            Observer,
            Backfill,
            PrintPIInfo
        };
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// 
        const string version = "1.2.0.0";
        private static readonly ILog logger = LogManager.GetLogger(typeof(Program));
        static void Main(string[] args)
        {
            GlobalContext.Properties["applicationName"] = "pi-connector";
            XmlConfigurator.Configure(new System.IO.FileInfo("log4net.config"));
            //Installer installer = new Installer();
            //installer.OnBeforeInstall(null, null);
            logger.Info($"TD PI start, version:{version}");

            if (args != null && args.Length == 1 && (args[0][0] == '-' || args[0][0] == '/'))
            {
                switch (args[0].Substring(1).ToLower())
                {
                    case "install":
                    case "i":
                        if (!ServiceInstallerUtility.InstallService())
                            logger.Fatal("Failed to install service");
                        break;
                    case "uninstall":
                    case "u":
                        if (!ServiceInstallerUtility.UninstallService())
                            logger.Fatal("Failed to uninstall service");
                        break;
                    default:
                        logger.Error("Unrecognized parameters (allowed: /install and /uninstall, shorten /i and /u)");
                        break;
                }
                Environment.Exit(0);
            }

            WorkMode workMode = WorkMode.Observer;

            string tomlConfigFile = "";
            string pointFilter = "*";
            if (args != null && args.Length >= 2)
            {
                for (int i = 0; i < args.Length; ) {
                    switch (args[i].Substring(1).ToLower())
                    {
                        case "file":
                        case "f":
                            tomlConfigFile = args[i + 1].Substring(0);
                            logger.Info($"toml config Path: {tomlConfigFile}");
                            i += 2;
                            break;
                        case "print":
                        case "p":
                            workMode = WorkMode.PrintPIInfo;
                            pointFilter = args[i + 1];
                            i += 2;
                            break;
                        default:
                            logger.Error("Unrecognized parameters");
                            Environment.Exit(0);
                            break;
                    }
                }
            }

            try
            {
                AppSettings.Init(tomlConfigFile);
            }
            catch (Exception ex)
            {
                logger.Fatal("Invalid JSON configuration for ", ex);
                return;
            }

            Service service = new Service();
            var servicesToRun = new ServiceBase[] { service };

            if (workMode == WorkMode.PrintPIInfo) {
                service.PrintPIInfo(pointFilter);
            }
            // console mode
            else if (Environment.UserInteractive)
            {
                logger.Info("Running in console mode");

                service.Start();

                while (true) {
                    var str = Console.ReadLine();
                    if (str == "quit")
                    {
                        logger.Info("TD PI Connector quit...");
                        break;
                    }
                    else {
                        Thread.Sleep(5000);
                    }
                }
                logger.Info("TD PI Connector quit.");
                service.Stop();
            }
            else
            {
                logger.Info("Running in service mode");
                ServiceBase.Run(servicesToRun);
            }
        }
    }
}

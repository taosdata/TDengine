using log4net;
using PISimulator.Core;
using System;
using System.ServiceProcess;

namespace PISimulator.Service
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// 
        private static readonly ILog logger = LogManager.GetLogger(typeof(Program));
        static void Main(string[] args)
        {
            //Installer installer = new Installer();
            //installer.OnBeforeInstall(null, null);

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



            try
            {
                AppSettings.Init();
            }
            catch (Exception ex)
            {
                logger.Fatal("Invalid JSON configuration for ", ex);
                return;
            }

            Service service = new Service();
            var servicesToRun = new ServiceBase[] { service };

            // console mode
            if (Environment.UserInteractive)
            {
                logger.Debug("Running in console mode");
                service.Start();

                Console.WriteLine("Press any key to stop the service...");
                Console.Read();

                service.Stop();
            }
            else
            {
                logger.Debug("Running in service mode");
                ServiceBase.Run(servicesToRun);
            }
        }
    }
}

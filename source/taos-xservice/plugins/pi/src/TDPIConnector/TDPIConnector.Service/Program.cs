using log4net;
using System;
using TDPIConnector.Core;
using TDPIConnector.Core.ScanPiInfo;
using System.Reflection;
using System.Linq;

namespace TDPIConnector.Service
{
    static class Program
    {
        enum WorkMode {
            Observer,
            Backfill,
            PrintPIInfo,
            CheckConfig
        };

        static private ILog logger;

        static void PrintVersion(bool writelog) {
            Assembly assembly = Assembly.GetExecutingAssembly();
            var cus_ttributes = assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>();
            var build_time = cus_ttributes.FirstOrDefault(a => a.Key == "BuildTime").Value;
            var commit = cus_ttributes.FirstOrDefault(a => a.Key == "Commit").Value;

            AssemblyName assemblyName = assembly.GetName();
            Version version = assemblyName.Version;

            if (writelog)
            {
                logger.Info("PI Connector version is: " + version);
                logger.Info("PI Connector commit is: " + commit);
                logger.Info("PI Connector build at: " + build_time);
            }
            else
            {
                Console.WriteLine("PI Connector");
                Console.WriteLine($"    Version : {version}");
                Console.WriteLine($"    Commit : {commit}");
                Console.WriteLine($"    Build Time : {build_time}");
            }
        }
        static void Main(string[] args)
        {
            if (args != null && args.Length == 1 && (args[0][0] == '-' || args[0][0] == '/'))
            {
                switch (args[0].Substring(1).ToLower())
                {
                    case "version":
                    case "v":
                        PrintVersion(false);
                        return;
                    case "piversion":
                    case "pv":
                        Service.PrintPISDKInfo();
                        return;
                    default:
                        Console.WriteLine("Unrecognized parameters (allowed: /install and /uninstall, shorten /i and /u)");
                        break;
                }
                Environment.Exit(0);
            }

            WorkMode workMode = WorkMode.Observer;
            ScanMode printMode = ScanMode.ScanNone;
            FilterMode fileterMode = FilterMode.FilterNone;


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
                            i += 2;
                            break;
                        case "print":
                        case "p":
                            workMode = WorkMode.PrintPIInfo;
                            printMode = ScanMode.ScanPIInfo;
                            pointFilter = args[i + 1];
                            i += 2;
                            break;
                        case "pp":
                            workMode = WorkMode.PrintPIInfo;
                            printMode = ScanMode.ScanPoint;
                            pointFilter = args[i + 1];
                            i += 2;
                            break;
                        case "px":
                            workMode = WorkMode.PrintPIInfo;
                            printMode = ScanMode.ScanPx;
                            pointFilter = args[i + 1];
                            fileterMode = PIInfoScanner.GetFilterMode(args[i + 2]);
                            i += 3;
                            break;
                        case "pt":
                            workMode = WorkMode.PrintPIInfo;
                            printMode = ScanMode.ScanPt;
                            pointFilter = args[i + 1];
                            fileterMode = PIInfoScanner.GetFilterMode(args[i + 2]);
                            i += 3;
                            break;
                        case "check":
                        case "c":
                            tomlConfigFile = args[i + 1].Substring(0);
                            workMode = WorkMode.CheckConfig;
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
                logger = AppSettings.log;
            }
            catch (Exception ex)
            {
                try
                {
                    Console.Error.WriteLine("Init Failed! Please check toml config file.");
                }
                catch { }

                string exType = "<unknown>";
                string exMessage = "<message unavailable>";
                string exStack = "<stacktrace unavailable>";
                try { exType = ex.GetType().FullName; } catch { }
                try { exMessage = ex.Message; } catch { }
                try { exStack = ex.StackTrace; } catch { }

                try { Console.Error.WriteLine($"Exception type: {exType}"); } catch { }
                try { Console.Error.WriteLine($"Exception message: {exMessage}"); } catch { }
                try { Console.Error.WriteLine($"Exception stacktrace: {exStack}"); } catch { }

                try { logger?.Fatal("Init Failed! Please check toml config file."); } catch { }
                try { if (logger != null) logger.Fatal($"Exception type: {exType}"); } catch { }
                try { if (logger != null) logger.Fatal($"Exception message: {exMessage}"); } catch { }
                try { if (logger != null) logger.Fatal($"Exception stacktrace: {exStack}"); } catch { }

                return;
            }

            PrintVersion(true);
            Service service = new Service();
            if (workMode == WorkMode.PrintPIInfo) {
                service.PrintPIInfo(printMode, pointFilter, fileterMode);
                return;
            } else if (workMode == WorkMode.CheckConfig) {
                service.CheckConfig();
                return;
            }
            // console mode
            else if (Environment.UserInteractive)
            {
                logger.Info("Running in console mode");
            }
            else
            {
                logger.Info("Running in service mode");
            }
            service.Start();
            service.Wait();
            service.Stop();
            logger.Info("Program quit.");
        }
    }
}

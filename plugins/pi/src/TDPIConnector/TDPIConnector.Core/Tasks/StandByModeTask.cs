using log4net;
using System;
using System.Threading;
using System.Threading.Tasks;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;

namespace TDPIConnector.Core.Tasks
{
    class StandByModeTask
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly AppService appService;
        private readonly Task task;
        private bool stopRequested;

        public StandByModeTask(AppService appService, PIServerManager piServerManager, TDEngineProxy tdEngineProxy)
        {
            this.appService = appService;
            this.task = new Task(async () =>
            {
                while (!stopRequested)
                {

                    if (StandbyManager.Instance.PIConnectionError && !stopRequested)
                    {
                        try
                        {
                            log.Debug("PI Connection error detected: Stopping data pipes");
                            this.appService.StopDataPipe();
                            piServerManager.Dispose();
                            log.Debug("PI Connection error detected: Connecting to PI");
                            piServerManager.Connect();
                            log.Debug("PI Connection error detected: Starting data pipes");
                            this.appService.StartDataPipe();
                            log.Debug("PI Connection error detected: Starting backfilling");
                            this.appService.StartBackfill();
                            log.Debug("Checking PI Data Archive connection: SUCCESS");
                        }
                        catch (Exception)
                        {
                            StandbyManager.Instance.ReportPIConnectionFailure();
                            log.Debug("Checking PI Data Archive connection: FAILED");
                        }
                        Thread.Sleep(5000);
                    }

                    if (StandbyManager.Instance.StandByModeEnabled && !stopRequested)
                    {
                        try
                        {
                            await tdEngineProxy.GetServerVersion();
                            log.Debug("Checking TDEngine connection: SUCCESS");
                        }
                        catch (Exception)
                        {
                            log.Debug("Checking TDEngine connection: FAILED");
                        }

                        Thread.Sleep(5000);
                    }

                    if (!stopRequested)
                    {
                        Thread.Sleep(1000);
                    }
                }
            });
        }


        public void Start()
        {
            log.Debug("Starting StandByModeTask...");
            this.task.Start();
            log.Info("StandByModeTask started successfully");
        }

        public void Stop()
        {
            log.Debug("Stopping StandByModeTask...");
            this.stopRequested = true;
            this.task.Wait();
            log.Info("StandByModeTask stopped successfully");
        }
    }
}

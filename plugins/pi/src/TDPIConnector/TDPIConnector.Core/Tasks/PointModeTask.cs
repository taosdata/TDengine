using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Monitoring;
using TDPIConnector.PI;
using TDPIConnector.PI.Exceptions;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Tasks
{
    class PointModeTask
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly PIServerManager piServerManager;
        private readonly IMonitoringService monitoringService;
        private readonly List<TDTable> piPoints;
        private readonly Task task;
        private PIDataPipeManager piDataPipeManager;
        private bool stopTaskRequested;

        public PointModeTask(PIServerManager piServerManager, 
            IMonitoringService monitoringService,
            PointModeObserver pointModeObserver,
            List<TDTable> piPoints)
        {
            stopTaskRequested = false;
            this.piServerManager = piServerManager;
            this.monitoringService = monitoringService;
            this.piPoints = piPoints;

            this.task = new Task(async() =>
            {
                List<string> piPointNames = this.piPoints.Select(p => p.Name).ToList();
                this.piDataPipeManager = this.piServerManager.AddSignups(piPointNames, pointModeObserver, AppSettings.tomlConfig.PIDataPipesInstances);
                log.Info("Process datapipe, PI Point Mode observer start...");
                while (!stopTaskRequested)
                {
                    if (!StandbyManager.Instance.PIConnectionError)
                    {
                        try
                        {
                            piDataPipeManager.GetObserverEvents();
                            await Task.Delay(AppSettings.tomlConfig.UpdateInterval);
                        }
                        catch (Exception ex)
                        {
                            // this.monitoringService.PublishPIException(ex);
                            if (ex is PIServerConnectionException)
                            {
                                log.Warn("PI Data Archive not available.");
                            }
                            else
                            {
                                log.Error("Error retrieving updates from point mode.", ex);
                            }
                        }
                    }
                }
            });
        }

        public void Start()
        {
            log.Debug("Starting PointModeTask...");
            stopTaskRequested = false;
            this.task.Start();
            log.Debug("PointModeTask started successfully");
        }

        public void Stop()
        {
            log.Debug("Stopping PointModeTask...");
            stopTaskRequested = true;
            this.task.Wait();
            if (piDataPipeManager != null)
            {
                piDataPipeManager.Dispose();
                piDataPipeManager = null;
            }
            log.Debug("PointModeTask stopped successfully");
        }
    }
}

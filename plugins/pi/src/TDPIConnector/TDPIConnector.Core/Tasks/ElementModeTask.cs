using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TDPIConnector.Core.Monitoring;
using TDPIConnector.PI;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.Core.Tasks
{
    class ElementModeTask
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly IMonitoringService monitoringService;
        private readonly Dictionary<string, AFElementWrapper> elements;
        private readonly Task task;
        private readonly PISystemManager piSystemManager;
        private AFDataPipeManager afDataPipeWrapper;
        private bool stopTaskRequested;

        public ElementModeTask(PISystemManager piSystemManager,
            IMonitoringService monitoringService,
            ElementModeObserver elementModeObserver,
            Dictionary<string, AFElementWrapper> elements)
        {
            stopTaskRequested = false;
            this.piSystemManager = piSystemManager;
            this.monitoringService = monitoringService;
            this.elements = elements;

            this.task = new Task(async () =>
            {
                log.Info("Process datapipe, AF Element Mode observer start...");
                this.afDataPipeWrapper = this.piSystemManager.AddSignups(this.elements.Values.ToList(), elementModeObserver, AppSettings.tomlConfig.AFDataPipesInstances);
                while (!stopTaskRequested)
                {
                    if (!StandbyManager.Instance.PIConnectionError)
                    {
                        try
                        {
                            afDataPipeWrapper.GetObserverEvents();
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
                                log.Error("Error retrieving updates from element mode.", ex);
                            }
                        }
                    }
                }
            });
        }

        public void Start()
        {
            log.Debug("Starting ElementModeTask...");
            stopTaskRequested = false;
            this.task.Start();
            log.Debug("ElementModeTask started successfully");
        }

        public void Stop()
        {
            log.Debug("Stopping ElementModeTask...");
            stopTaskRequested = true;
            this.task.Wait();
            if (afDataPipeWrapper != null)
            {
                afDataPipeWrapper.Dispose();
            }
            log.Debug("ElementModeTask stopped successfully");
        }
    }
}

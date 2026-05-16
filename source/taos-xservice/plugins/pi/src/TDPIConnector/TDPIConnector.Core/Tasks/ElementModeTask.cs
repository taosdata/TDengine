using log4net;
using System;
using System.Linq;
using System.Threading.Tasks;
using OSIsoft.AF.Asset;
using TDPIConnector.PI;
using TDPIConnector.PI.Exceptions;
using System.Threading;
using System.Collections.Concurrent;

namespace TDPIConnector.Core.Tasks
{
    public class ElementModeTask
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly ConcurrentDictionary<string, AFElementWrapper> elements;
        private readonly Task task;
        private readonly PISystemManager piSystemManager;
        private AFDataPipeManager afDataPipeWrapper;
        private bool stopTaskRequested;
        private Semaphore semSignup = new Semaphore(0, 1);

        public ElementModeTask(PISystemManager piSystemManager,
            ElementModeObserver elementModeObserver,
            ConcurrentDictionary<string, AFElementWrapper> elements)
        {
            stopTaskRequested = false;
            this.piSystemManager = piSystemManager;
            this.elements = elements;

            this.task = new Task(async () =>
            {
                log.Info("Process datapipe, AF Element Mode observer startting...");
                try {
                    this.afDataPipeWrapper = this.piSystemManager.AddSignups(this.elements.Values.ToList(), elementModeObserver, AppSettings.tomlConfig.AFDataPipesInstances);
                }
                catch (Exception e)
                {
                    log.Error("Error Occured when AF Element AddSignups.", e);
                    stopTaskRequested = true;
                }
                log.Info($"Process datapipe, AF Element Mode observer end. element count:{this.elements.Count()}.");
                semSignup.Release();
                StartEventsObserver();
            });
        }
        public void SignUpBatchAttributes(string templateName, ref System.Collections.Generic.List<AFAttribute> attributes)
        {
            try
            {
                afDataPipeWrapper.AddSignupAttributes(ref templateName, ref attributes);
            }
            catch (Exception e) {
                log.Error($"SignUp failed! Will retry one by one! {e.Message}");
                afDataPipeWrapper.RetrySignUpBatchAttributes(ref templateName, ref attributes);
            }
        }
        private void StartEventsObserver() {
            while (!stopTaskRequested)
            {
                if (!StandbyManager.Instance.PIConnectionError)
                {
                    try
                    {
                        afDataPipeWrapper.GetObserverEvents();
                        // Task.Delay(AppSettings.tomlConfig.UpdateInterval);
                        Thread.Sleep(AppSettings.tomlConfig.UpdateInterval);
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
        }

        public ElementModeTask(PISystemManager piSystemManager,
            ElementModeObserver elementModeObserver)
        {
            stopTaskRequested = false;
            this.piSystemManager = piSystemManager;

            log.Info("Process datapipe, AF Element Mode observer startting...");
            task = new Task(() =>
            {
                afDataPipeWrapper = piSystemManager.InitSignuper(elementModeObserver, AppSettings.tomlConfig.AFDataPipesInstances);

                StartEventsObserver();
            });
            semSignup.Release();
        }


        public void Start()
        {
            log.Debug("Starting ElementModeTask...");
            stopTaskRequested = false;
            task.Start();
            semSignup.WaitOne();
            log.Debug("ElementModeTask started successfully");
        }

        public void Stop()
        {
            log.Debug("Stopping ElementModeTask...");
            stopTaskRequested = true;
            task.Wait();
            if (afDataPipeWrapper != null)
            {
                afDataPipeWrapper.Dispose();
            }
            log.Debug("ElementModeTask stopped successfully");
        }
    }
}

using log4net;
using System;
using System.Threading;
using System.Threading.Tasks;
using TDPIConnector.TDEngine.Exceptions;

namespace TDPIConnector.Core.Tasks
{
    class EventsSenderTask
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly EventsSender eventsSender;
        private readonly Task task;
        private bool stopRequested;

        public EventsSenderTask(EventsSender eventsSender)
        {
            this.eventsSender = eventsSender;

            task = new Task(() =>
            {
                log.Info("Process datapipe, Event sender start...");
                while (!stopRequested)
                {
                    try
                    {
                        eventsSender.OnAFElementEvents();
                        eventsSender.OnPIPointEvents();
                    }
                    catch (Exception ex)
                    {
                        if (ex.InnerException is TDEngineTimeoutException)
                        {
                            log.Warn("TDEngine not available.");
                        }
                        else
                        {
                            log.Error("Error sending data to TDEngine.", ex);
                        }
                    }
                    finally
                    {
                        Thread.Sleep(AppSettings.tomlConfig.UpdateInterval);
                        // await Task.Delay(AppSettings.tomlConfig.UpdateInterval);
                    }
                }
                log.Info("Process datapipe, Event sender stop.");
            });
        }


        public void Start()
        {
            log.Debug("Starting EventsSenderTask...");
            this.task.Start();
            log.Debug("EventsSenderTask started successfully");
        }

        public void Stop()
        {
            log.Debug("Stopping EventsSenderTask...");
            this.stopRequested = true;
            this.task.Wait();
            log.Debug("EventsSenderTask stopped successfully");
        }
    }
}

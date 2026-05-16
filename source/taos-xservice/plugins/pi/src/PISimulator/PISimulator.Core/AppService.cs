using log4net;
using System;
using System.Threading.Tasks;


namespace PISimulator.Core
{
    public class AppService
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private bool stopRequested;
        private PISystemManager piSystemManager = null;
        private Task task;

        public AppService()
        {
            this.task = new Task(async () =>
            {
                while (!stopRequested)
                {
                    try
                    {
                        log.Info("New cycle...");
                        piSystemManager.SendValues(AppSettings.AFSettingsConfig);
                        await Task.Delay(AppSettings.UpdateInterval);
                    }
                    catch (Exception ex)
                    {
                      
                        log.Error("Error retrieving updates.", ex);
                    }
                }
            });
        }

        public void Start()
        {
            try
            {
                piSystemManager = new PISystemManager(AppSettings.PISystemName);
                piSystemManager.Connect();
                piSystemManager.CreateAssets(AppSettings.AFSettingsConfig);
                task.Start();
            }
            catch (Exception e)
            {
                log.Fatal("Error starting the application.", e);
                throw e;
            }
        }

        public void Stop()
        {
            stopRequested = true;
            task.Wait();          
            piSystemManager.Dispose();
        }
    }
}

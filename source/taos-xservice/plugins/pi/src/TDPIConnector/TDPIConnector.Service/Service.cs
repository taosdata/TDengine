using log4net;
using System.ServiceProcess;
using TDPIConnector.Core;
using TDPIConnector.Core.ScanPiInfo;
using System.Threading.Tasks;

namespace TDPIConnector.Service
{
    public partial class Service : ServiceBase
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private AppService appService;
        private volatile bool _started;
        private Task _runTask;

        public Service()
        {
            InitializeComponent();
            appService = new AppService();
        }

        protected override void OnStart(string[] args)
        {
            if (_started)
            {
                log.Warn("Service already started");
                return;
            }
            _started = true;
            log.Info("Start event");
            _runTask = Task.Run(async () =>
            {
                try
                {
                    await appService.Start(); // 启动服务
                    log.Info("Wait event begin...");
                    appService.Wait();  // 在服务模式下，这里会阻塞直到服务停止
                    log.Info("Wait event finish.");
                }
                catch (System.Exception e)
                {
                    log.Fatal("Service start failed", e);
                    try { Stop(); } catch { }
                }
            });
        }

        public void Wait()
        {
            log.Info("Wait event begin...");
            appService.Wait();
            log.Info("Wait event finish.");
        }

        protected override void OnStop()
        {
            if (!_started)
            {
                log.Warn("Service not started");
                return;
            }
            log.Info("Stop event");
            appService.Stop();
            try
            {
                // 等待后台任务结束
                _runTask?.Wait(5000);
            }
            catch (System.Exception ex)
            {
                log.Warn("Wait service stop task failed", ex);
            }
            _started = false;
        }

        protected override void OnShutdown()
        {
            log.Info("Windows is going shutdown");
            appService.Stop();
        }

        public void Start()
        {
            OnStart(null);
        }
        public void PrintPIInfo(ScanMode scanMode, string filter, FilterMode filterMode)
        {
            log.Info("Start Print PI Info");
            appService.PrintPIInfo(scanMode, filter, filterMode);
        }
        public void CheckConfig()
        {
            log.Info("Start Check Points and Templates");
            appService.CheckConfig();
        }
        public static void PrintPISDKInfo()
        {
            AppService.GetPISDKInfo();
        }
    }
}

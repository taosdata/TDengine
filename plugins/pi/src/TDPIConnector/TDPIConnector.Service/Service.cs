using log4net;
using System.ServiceProcess;
using TDPIConnector.Core;
using TDPIConnector.Core.ScanPiInfo;

namespace TDPIConnector.Service
{
    public partial class Service : ServiceBase
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private AppService appService;

        public Service()
        {
            InitializeComponent();
            appService = new AppService();
        }

        protected override void OnStart(string[] args)
        {

            log.Info("Start event");
            appService.Start();
        }

        protected override void OnStop()
        {
            log.Info("Stop event");
            appService.Stop();
        }

        protected override void OnShutdown()
        {
            log.Info("Windows is going shutdown");
            Stop();
        }


        public void Start()
        {
            OnStart(null);
        }
        public void PrintPIInfo(ScanMode scanMode, string filter, FilterMode filterMode) {
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
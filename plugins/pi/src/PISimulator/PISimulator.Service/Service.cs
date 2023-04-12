using log4net;
using PISimulator.Core;
using System.ServiceProcess;

namespace PISimulator.Service
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
    }
}
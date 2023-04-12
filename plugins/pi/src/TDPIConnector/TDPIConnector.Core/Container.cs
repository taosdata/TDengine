using Ninject;
using TDPIConnector.Core.Monitoring;

namespace TDPIConnector.Core
{
    public class Container
    {
        private static StandardKernel kernel = null;
        public static IKernel CreateKernel()
        {
            kernel = new StandardKernel();
            kernel.Bind<IMonitoringService>().To<MonitoringService>().InSingletonScope();
            return kernel;
        }

        public static T Resolve<T>()
        {
            return kernel.Get<T>();
        }
    }
}

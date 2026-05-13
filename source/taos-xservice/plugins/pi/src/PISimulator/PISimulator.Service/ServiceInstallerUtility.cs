using System.Configuration.Install;
using System.Reflection;

namespace PISimulator.Service
{
    public class ServiceInstallerUtility
    {
        private static readonly string exePath = Assembly.GetExecutingAssembly().Location;
        public static bool InstallService()
        {
            try { 
                ManagedInstallerClass.InstallHelper(new[] { "/LogFile=", exePath });
                return true;
            }
            catch { 
                return false; 
            }        
        }
        public static bool UninstallService()
        {
            try { 
                ManagedInstallerClass.InstallHelper(new[] { "/LogFile=", "/u", exePath });
                return true;
            }
            catch { 
                return false; 
            }          
        }
    }
}

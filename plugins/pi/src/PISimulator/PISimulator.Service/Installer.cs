using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace PISimulator.Service
{
    [RunInstaller(true)]
    public partial class Installer : System.Configuration.Install.Installer
    {
        public Installer()
        {
            InitializeComponent();
        }

        private void OnAfterInstall(object sender, InstallEventArgs e)
        {
            try
            {
                using (var sc = new ServiceController(serviceInstaller.ServiceName))
                {
                    if (sc != null)
                    {
                        sc.Start();
                    }
                }
            }
            catch (Exception)
            {

            }
        }

        private void OnBeforeUninstall(object sender, InstallEventArgs e)
        {
            try
            {
                using (var sc = new ServiceController(serviceInstaller.ServiceName))
                {
                    if (sc != null)
                    {
                        sc.Stop();
                    }
                }
            }
            catch (Exception)
            {

            }
        }

        public void OnBeforeInstall(object sender, InstallEventArgs e)
        {
            var serviceController = ServiceController.GetServices().FirstOrDefault(s => s.ServiceName.Equals(serviceInstaller.ServiceName));
            if (serviceController != null)
            {
                if (serviceController.Status == ServiceControllerStatus.Running)
                {
                    serviceController.Stop();
                }
                var serviceInstallerObj = new ServiceInstaller();
                var context = new InstallContext();
                serviceInstallerObj.Context = context;
                serviceInstallerObj.ServiceName = serviceInstaller.ServiceName;
                serviceInstallerObj.Uninstall(null);
            }
        }
    }
}


using OSIsoft.AF;
using System;
using System.Data;
using System.Linq;
using System.Windows.Forms;

namespace TDEngineDR.Setup
{
    public partial class WindowsForm : Form
    {
        private PISystem piSystem;
        private AFPlugIn afPlugin;

        public WindowsForm()
        {
            InitializeComponent();
            grAF.Visible = false;
            grPlugin.Visible = false;
            btnAct.Visible = false;
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            try
            {

                this.piSystem = this.piSystemPicker1.PISystem;
                this.piSystem.Connect();
                this.FillData();
                grAF.Visible = true;
                grPlugin.Visible = true;
                btnAct.Visible = true;
            }
            catch(Exception ex)
            {
                MessageBox.Show("Could not connect to the PI System. " + ex.Message, "Error");
            }
          
        }

        private void FillData()
        {

            this.lbName.Text = "Name: " + this.piSystem.Name;
            this.lbAccountName.Text = "Account: " + this.piSystem.CurrentUserName;
            this.lbVersion.Text = "Version: " + this.piSystem.ServerVersion;
            this.UpdateButtonAct();


        }

        private void UpdateButtonAct()
        {
            try
            {

                this.piSystem.Refresh();
                this.afPlugin = this.piSystem.DataReferencePlugIns.Where(p => p.Name == "TDengine").FirstOrDefault();
                if (afPlugin == null)
                {
                    this.btnAct.Text = "Install";
                    this.lbPluginName.Text = "Plugin not found";
                    this.lbPluginVersion.Text = string.Empty;
                }
                else
                {
                    this.btnAct.Text = "Uninstall";
                    this.lbPluginName.Text = "Plugin Name: " + afPlugin.Name;
                    this.lbPluginVersion.Text = "Plugin Version: " + afPlugin.Version;
                }
            }
            catch(Exception ex)
            {
                MessageBox.Show("Could not connect to the PI System. " + ex.Message);
            }
        }

        private void btnAct_Click(object sender, EventArgs e)
        {
            this.piSystem.Refresh();
            if (afPlugin == null)
            {
                try
                {
                    string folderPath = AppDomain.CurrentDomain.BaseDirectory;
                    this.piSystem.UploadPlugInAssembly(folderPath + "TDEngineDR.dll", true);
                    this.piSystem.Refresh();
                    this.afPlugin = this.piSystem.DataReferencePlugIns.Where(p => p.Name == "TDengine").SingleOrDefault();
                    this.piSystem.UploadPlugInSupportAssembly(this.afPlugin.AssemblyID, "4.0", folderPath + "Newtonsoft.Json.dll", true);
                }
                catch(Exception ex)
                {
                    MessageBox.Show("Could not upload plugin to the PI System. " + ex.Message, "Error");
                }

            }
            else
            {
                try
                {
                    foreach (var supportAssembly in this.afPlugin.SupportAssemblies)
                    {
                        this.piSystem.RemovePlugInSupportAssembly(supportAssembly.AssemblyID, this.afPlugin.AssemblyID);
                    }
                    this.piSystem.RemovePlugInAssembly(this.afPlugin.AssemblyID);
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Could not remove plugin to the PI System. " + ex.Message, "Error");
                }
            }
            UpdateButtonAct();
        }
    }
}

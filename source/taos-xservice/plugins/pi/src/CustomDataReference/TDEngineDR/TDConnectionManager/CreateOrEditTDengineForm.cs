using System;
using System.Threading.Tasks;
using System.Windows.Forms;
using TDConnectionManager.Models;

namespace TDConnectionManager
{
    public partial class CreateOrEditTDengineForm : Form
    {
        private WindowForm windowForm;
        private TDengineServer tdEngineServer;
        private bool editMode;

        public CreateOrEditTDengineForm(WindowForm windowForm, TDengineServer tdEngineServer)
        {
            InitializeComponent();
            this.windowForm = windowForm;
            this.tdEngineServer = tdEngineServer;
            this.editMode = tdEngineServer != null;
            rbOnCloud.Checked = true;
            if (tdEngineServer != null)
            {
                tbName.Text = tdEngineServer.Name;
                tbHost.Text = tdEngineServer.Host;
                tbPort.Text = tdEngineServer.Port.ToString();

                if (!tdEngineServer.IsCloud)
                {
                    rbOnPrem.Checked = true;
                    rbOnCloud.Checked = false;
                    tbUser.Text = tdEngineServer.Username.ToString();
                    tbPassword.Text = tdEngineServer.Password.ToString();
                }
                else
                {
                    rbOnPrem.Checked = false;
                    rbOnCloud.Checked = true;
                    tbToken.Text = tdEngineServer.Token.ToString();
                }

            }
            this.tbName.Enabled = !editMode;
            this.rbOnCloud.Enabled = !editMode;
            this.rbOnPrem.Enabled = !editMode;
            this.btnOK.Click += new System.EventHandler(async (s, e) => await this.btnOK_Click(s, e));

        }

        private void CreditOrEditTDengineForm_Load(object sender, EventArgs e)
        {

        }

        public TDengineServer CreateTDengineServer()
        {
            if (tdEngineServer == null)
            {
                tdEngineServer = new TDengineServer();
            }
            tdEngineServer.Name = tbName.Text;
            tdEngineServer.Host = tbHost.Text;
            tdEngineServer.Port = Convert.ToInt32(tbPort.Text);
            tdEngineServer.IsCloud = rbOnCloud.Checked;
            if (tdEngineServer.IsCloud)
            {
                tdEngineServer.Token = tbToken.Text;
            }
            else
            {
                tdEngineServer.Username = tbUser.Text;
                tdEngineServer.Password = tbPassword.Text;
            }
            return tdEngineServer;
        }

        private void rbOnPrem_CheckedChanged(object sender, EventArgs e)
        {
            lbUser.Visible = true;
            tbUser.Visible = true;
            lbPassword.Visible = true;
            tbPassword.Visible = true;
            lbToken.Visible = false;
            tbToken.Visible = false;
            tbPort.Text = "6041";
        }

        private void rbOnCloud_CheckedChanged(object sender, EventArgs e)
        {
            lbUser.Visible = false;
            tbUser.Visible = false;
            lbPassword.Visible = false;
            tbPassword.Visible = false;
            lbToken.Visible = true;
            tbToken.Visible = true;
            tbPort.Text = "443";
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            this.Close();
        }




        private async Task btnOK_Click(object sender, EventArgs e)
        {
            
            TDengineServer tdEngineServer = CreateTDengineServer();
            if (string.IsNullOrEmpty(tdEngineServer.Name))
            {
                MessageBox.Show("Server name can't be empty.", "Error");
                return;
            }
            HttpConnectionTester httpConnectionTester = new HttpConnectionTester(tdEngineServer);
            bool success = await httpConnectionTester.TestConnection();
            if (success)
            {
                MessageBox.Show("Connected successfully to TDengine!", "Success!");
                try
                {
                    this.windowForm.Save(tdEngineServer);
                    this.Close();
                }
                catch(Exception ex)
                {
                    MessageBox.Show(ex.Message, "Error");
                    return;
                }
            }
            else
            {
                MessageBox.Show("Failed to connect to TDengine!", "Error");
            }
        }
    }
}

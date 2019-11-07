using System;
using System.Windows.Forms;

namespace excel2010
{
    public partial class TDConnectForm : TDForm
    {
        public TDConnectForm()
        {
            InitializeComponent();
        }

        public override void Initialize()
        {
            this.urlTextBox.Text = Globals.ThisAddIn.tdPersist.URL;
            this.databaseTextBox.Text = Globals.ThisAddIn.tdPersist.DB;
            this.usernameTextBox.Text = Globals.ThisAddIn.tdPersist.USER;
            this.passwordTextBox.Text = Globals.ThisAddIn.tdPersist.PASS;
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.URL = this.urlTextBox.Text;
            Globals.ThisAddIn.tdPersist.DB = this.databaseTextBox.Text;
            Globals.ThisAddIn.tdPersist.USER = this.usernameTextBox.Text;
            Globals.ThisAddIn.tdPersist.PASS = this.passwordTextBox.Text;
        }

        private void Form_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == (char)Keys.Escape)
            {
                this.GetFactory().CloseForm();
            }
        }

        private void SaveButton_Click(object sender, EventArgs e)
        {
            Globals.ThisAddIn.tdPersist.URL = this.urlTextBox.Text;
            Globals.ThisAddIn.tdPersist.DB = this.databaseTextBox.Text;
            Globals.ThisAddIn.tdPersist.USER = this.usernameTextBox.Text;
            Globals.ThisAddIn.tdPersist.PASS = this.passwordTextBox.Text;

            if (Globals.ThisAddIn.tdPersist.URL.Length <= 0 || Globals.ThisAddIn.tdPersist.URL.Length >= 64)
            {
                Globals.ThisAddIn.tdUtil.ShowError("invalid url");
                return;
            }

            if (Globals.ThisAddIn.tdPersist.DB.Length <= 0 || Globals.ThisAddIn.tdPersist.DB.Length >= 32)
            {
                Globals.ThisAddIn.tdUtil.ShowError("invalid database name");
                return;
            }

            if (Globals.ThisAddIn.tdPersist.USER.Length <= 0 || Globals.ThisAddIn.tdPersist.USER.Length >= 32)
            {
                Globals.ThisAddIn.tdUtil.ShowError("invalid user name");
                return;
            }

            if (Globals.ThisAddIn.tdPersist.PASS.Length <= 0 || Globals.ThisAddIn.tdPersist.PASS.Length >= 32)
            {
                Globals.ThisAddIn.tdUtil.ShowError("invalid password");
                return;
            }

            if (Globals.ThisAddIn.tdHttp.DoLogin())
            {
                Globals.ThisAddIn.tdUtil.ShowInfo("connect success");
                //this.GetFactory().CloseForm();
            }
        }
    }
}


namespace TDConnectionManager
{
    partial class WindowForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(WindowForm));
            this.lbTDServers = new System.Windows.Forms.ListBox();
            this.lbList = new System.Windows.Forms.Label();
            this.grTDServerInfo = new System.Windows.Forms.GroupBox();
            this.lbPassword = new System.Windows.Forms.Label();
            this.lbToken = new System.Windows.Forms.Label();
            this.lbUser = new System.Windows.Forms.Label();
            this.lbPort = new System.Windows.Forms.Label();
            this.lbHost = new System.Windows.Forms.Label();
            this.lbName = new System.Windows.Forms.Label();
            this.piSystemPicker1 = new OSIsoft.AF.UI.PISystemPicker();
            this.btnConnect = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.btnEdit = new System.Windows.Forms.Button();
            this.btnAdd = new System.Windows.Forms.Button();
            this.btnDelete = new System.Windows.Forms.Button();
            this.grTDServerInfo.SuspendLayout();
            this.SuspendLayout();
            // 
            // lbTDServers
            // 
            this.lbTDServers.FormattingEnabled = true;
            this.lbTDServers.Location = new System.Drawing.Point(53, 139);
            this.lbTDServers.Name = "lbTDServers";
            this.lbTDServers.Size = new System.Drawing.Size(333, 316);
            this.lbTDServers.TabIndex = 0;
            this.lbTDServers.SelectedIndexChanged += new System.EventHandler(this.lbTDServers_SelectedIndexChanged);
            // 
            // lbList
            // 
            this.lbList.AutoSize = true;
            this.lbList.Location = new System.Drawing.Point(50, 110);
            this.lbList.Name = "lbList";
            this.lbList.Size = new System.Drawing.Size(296, 13);
            this.lbList.TabIndex = 1;
            this.lbList.Text = "List of TDengine servers from the Configuration AF Database.";
            // 
            // grTDServerInfo
            // 
            this.grTDServerInfo.Controls.Add(this.lbPassword);
            this.grTDServerInfo.Controls.Add(this.lbToken);
            this.grTDServerInfo.Controls.Add(this.lbUser);
            this.grTDServerInfo.Controls.Add(this.lbPort);
            this.grTDServerInfo.Controls.Add(this.lbHost);
            this.grTDServerInfo.Controls.Add(this.lbName);
            this.grTDServerInfo.Location = new System.Drawing.Point(437, 139);
            this.grTDServerInfo.Name = "grTDServerInfo";
            this.grTDServerInfo.Size = new System.Drawing.Size(374, 306);
            this.grTDServerInfo.TabIndex = 2;
            this.grTDServerInfo.TabStop = false;
            this.grTDServerInfo.Text = "TDengine Server details";
            // 
            // lbPassword
            // 
            this.lbPassword.AutoSize = true;
            this.lbPassword.Location = new System.Drawing.Point(54, 210);
            this.lbPassword.Name = "lbPassword";
            this.lbPassword.Size = new System.Drawing.Size(59, 13);
            this.lbPassword.TabIndex = 0;
            this.lbPassword.Text = "Password: ";
            // 
            // lbToken
            // 
            this.lbToken.AutoSize = true;
            this.lbToken.Location = new System.Drawing.Point(54, 170);
            this.lbToken.Name = "lbToken";
            this.lbToken.Size = new System.Drawing.Size(44, 13);
            this.lbToken.TabIndex = 0;
            this.lbToken.Text = "Token: ";
            // 
            // lbUser
            // 
            this.lbUser.AutoSize = true;
            this.lbUser.Location = new System.Drawing.Point(54, 170);
            this.lbUser.Name = "lbUser";
            this.lbUser.Size = new System.Drawing.Size(61, 13);
            this.lbUser.TabIndex = 0;
            this.lbUser.Text = "Username: ";
            // 
            // lbPort
            // 
            this.lbPort.AutoSize = true;
            this.lbPort.Location = new System.Drawing.Point(54, 126);
            this.lbPort.Name = "lbPort";
            this.lbPort.Size = new System.Drawing.Size(32, 13);
            this.lbPort.TabIndex = 0;
            this.lbPort.Text = "Port: ";
            // 
            // lbHost
            // 
            this.lbHost.AutoSize = true;
            this.lbHost.Location = new System.Drawing.Point(54, 84);
            this.lbHost.Name = "lbHost";
            this.lbHost.Size = new System.Drawing.Size(35, 13);
            this.lbHost.TabIndex = 0;
            this.lbHost.Text = "Host: ";
            // 
            // lbName
            // 
            this.lbName.AutoSize = true;
            this.lbName.Location = new System.Drawing.Point(54, 46);
            this.lbName.Name = "lbName";
            this.lbName.Size = new System.Drawing.Size(41, 13);
            this.lbName.TabIndex = 0;
            this.lbName.Text = "Name: ";
            // 
            // piSystemPicker1
            // 
            this.piSystemPicker1.AccessibleDescription = "PI System Picker";
            this.piSystemPicker1.AccessibleName = "PI System Picker";
            this.piSystemPicker1.Cursor = System.Windows.Forms.Cursors.Default;
            this.piSystemPicker1.Location = new System.Drawing.Point(53, 53);
            this.piSystemPicker1.LoginPromptSetting = OSIsoft.AF.UI.PISystemPicker.LoginPromptSettingOptions.Default;
            this.piSystemPicker1.Name = "piSystemPicker1";
            this.piSystemPicker1.ShowBegin = false;
            this.piSystemPicker1.ShowConnect = false;
            this.piSystemPicker1.ShowDelete = false;
            this.piSystemPicker1.ShowEnd = false;
            this.piSystemPicker1.ShowFind = false;
            this.piSystemPicker1.ShowList = false;
            this.piSystemPicker1.ShowNavigation = false;
            this.piSystemPicker1.ShowNew = false;
            this.piSystemPicker1.ShowNext = false;
            this.piSystemPicker1.ShowNoEntries = false;
            this.piSystemPicker1.ShowPrevious = false;
            this.piSystemPicker1.ShowProperties = false;
            this.piSystemPicker1.Size = new System.Drawing.Size(333, 22);
            this.piSystemPicker1.TabIndex = 3;
            // 
            // btnConnect
            // 
            this.btnConnect.Location = new System.Drawing.Point(408, 52);
            this.btnConnect.Name = "btnConnect";
            this.btnConnect.Size = new System.Drawing.Size(75, 23);
            this.btnConnect.TabIndex = 4;
            this.btnConnect.Text = "Connect";
            this.btnConnect.UseVisualStyleBackColor = true;
            this.btnConnect.Click += new System.EventHandler(this.btnConnect_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(50, 25);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(162, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Select the AF Server to connect:";
            // 
            // btnEdit
            // 
            this.btnEdit.Location = new System.Drawing.Point(451, 480);
            this.btnEdit.Name = "btnEdit";
            this.btnEdit.Size = new System.Drawing.Size(75, 23);
            this.btnEdit.TabIndex = 4;
            this.btnEdit.Text = "Edit";
            this.btnEdit.UseVisualStyleBackColor = true;
            this.btnEdit.Click += new System.EventHandler(this.btnEdit_Click);
            // 
            // btnAdd
            // 
            this.btnAdd.Location = new System.Drawing.Point(180, 480);
            this.btnAdd.Name = "btnAdd";
            this.btnAdd.Size = new System.Drawing.Size(75, 23);
            this.btnAdd.TabIndex = 4;
            this.btnAdd.Text = "Add";
            this.btnAdd.UseVisualStyleBackColor = true;
            this.btnAdd.Click += new System.EventHandler(this.btnAdd_Click);
            // 
            // btnDelete
            // 
            this.btnDelete.Location = new System.Drawing.Point(584, 480);
            this.btnDelete.Name = "btnDelete";
            this.btnDelete.Size = new System.Drawing.Size(75, 23);
            this.btnDelete.TabIndex = 4;
            this.btnDelete.Text = "Delete";
            this.btnDelete.UseVisualStyleBackColor = true;
            this.btnDelete.Click += new System.EventHandler(this.btnDelete_Click);
            // 
            // WindowForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(869, 524);
            this.Controls.Add(this.btnAdd);
            this.Controls.Add(this.btnDelete);
            this.Controls.Add(this.btnEdit);
            this.Controls.Add(this.btnConnect);
            this.Controls.Add(this.piSystemPicker1);
            this.Controls.Add(this.grTDServerInfo);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.lbList);
            this.Controls.Add(this.lbTDServers);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "WindowForm";
            this.Text = "PI TDengine Connection Manager";
            this.grTDServerInfo.ResumeLayout(false);
            this.grTDServerInfo.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ListBox lbTDServers;
        private System.Windows.Forms.Label lbList;
        private System.Windows.Forms.GroupBox grTDServerInfo;
        private System.Windows.Forms.Label lbToken;
        private System.Windows.Forms.Label lbUser;
        private System.Windows.Forms.Label lbPort;
        private System.Windows.Forms.Label lbHost;
        private System.Windows.Forms.Label lbName;
        private System.Windows.Forms.Label lbPassword;
        private OSIsoft.AF.UI.PISystemPicker piSystemPicker1;
        private System.Windows.Forms.Button btnConnect;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button btnEdit;
        private System.Windows.Forms.Button btnAdd;
        private System.Windows.Forms.Button btnDelete;
    }
}


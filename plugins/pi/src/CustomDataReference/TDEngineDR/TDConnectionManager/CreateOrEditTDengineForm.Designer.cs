
namespace TDConnectionManager
{
    partial class CreateOrEditTDengineForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(CreateOrEditTDengineForm));
            this.btnOK = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.lbHost = new System.Windows.Forms.Label();
            this.lbPort = new System.Windows.Forms.Label();
            this.lbUser = new System.Windows.Forms.Label();
            this.tbHost = new System.Windows.Forms.TextBox();
            this.tbPort = new System.Windows.Forms.TextBox();
            this.rbOnPrem = new System.Windows.Forms.RadioButton();
            this.rbOnCloud = new System.Windows.Forms.RadioButton();
            this.tbUser = new System.Windows.Forms.TextBox();
            this.tbPassword = new System.Windows.Forms.TextBox();
            this.lbPassword = new System.Windows.Forms.Label();
            this.lbToken = new System.Windows.Forms.Label();
            this.tbToken = new System.Windows.Forms.TextBox();
            this.lbServerType = new System.Windows.Forms.Label();
            this.lbName = new System.Windows.Forms.Label();
            this.tbName = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // btnOK
            // 
            this.btnOK.Location = new System.Drawing.Point(57, 370);
            this.btnOK.Name = "btnOK";
            this.btnOK.Size = new System.Drawing.Size(64, 23);
            this.btnOK.TabIndex = 0;
            this.btnOK.Text = "Ok";
            this.btnOK.UseVisualStyleBackColor = true;
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(221, 370);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(64, 23);
            this.btnCancel.TabIndex = 0;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // lbHost
            // 
            this.lbHost.AutoSize = true;
            this.lbHost.Location = new System.Drawing.Point(57, 65);
            this.lbHost.Name = "lbHost";
            this.lbHost.Size = new System.Drawing.Size(116, 13);
            this.lbHost.TabIndex = 1;
            this.lbHost.Text = "TDengine Server Host:";
            // 
            // lbPort
            // 
            this.lbPort.AutoSize = true;
            this.lbPort.Location = new System.Drawing.Point(54, 115);
            this.lbPort.Name = "lbPort";
            this.lbPort.Size = new System.Drawing.Size(113, 13);
            this.lbPort.TabIndex = 1;
            this.lbPort.Text = "TDengine Server Port:";
            // 
            // lbUser
            // 
            this.lbUser.AutoSize = true;
            this.lbUser.Location = new System.Drawing.Point(54, 234);
            this.lbUser.Name = "lbUser";
            this.lbUser.Size = new System.Drawing.Size(58, 13);
            this.lbUser.TabIndex = 1;
            this.lbUser.Text = "Username:";
            // 
            // tbHost
            // 
            this.tbHost.Location = new System.Drawing.Point(57, 81);
            this.tbHost.Name = "tbHost";
            this.tbHost.Size = new System.Drawing.Size(260, 20);
            this.tbHost.TabIndex = 2;
            // 
            // tbPort
            // 
            this.tbPort.Location = new System.Drawing.Point(57, 131);
            this.tbPort.Name = "tbPort";
            this.tbPort.Size = new System.Drawing.Size(260, 20);
            this.tbPort.TabIndex = 2;
            // 
            // rbOnPrem
            // 
            this.rbOnPrem.AutoSize = true;
            this.rbOnPrem.Location = new System.Drawing.Point(163, 194);
            this.rbOnPrem.Name = "rbOnPrem";
            this.rbOnPrem.Size = new System.Drawing.Size(79, 17);
            this.rbOnPrem.TabIndex = 3;
            this.rbOnPrem.TabStop = true;
            this.rbOnPrem.Text = "On Premise";
            this.rbOnPrem.UseVisualStyleBackColor = true;
            this.rbOnPrem.CheckedChanged += new System.EventHandler(this.rbOnPrem_CheckedChanged);
            // 
            // rbOnCloud
            // 
            this.rbOnCloud.AutoSize = true;
            this.rbOnCloud.Location = new System.Drawing.Point(60, 194);
            this.rbOnCloud.Name = "rbOnCloud";
            this.rbOnCloud.Size = new System.Drawing.Size(52, 17);
            this.rbOnCloud.TabIndex = 3;
            this.rbOnCloud.TabStop = true;
            this.rbOnCloud.Text = "Cloud";
            this.rbOnCloud.UseVisualStyleBackColor = true;
            this.rbOnCloud.CheckedChanged += new System.EventHandler(this.rbOnCloud_CheckedChanged);
            // 
            // tbUser
            // 
            this.tbUser.Location = new System.Drawing.Point(57, 250);
            this.tbUser.Name = "tbUser";
            this.tbUser.Size = new System.Drawing.Size(260, 20);
            this.tbUser.TabIndex = 2;
            // 
            // tbPassword
            // 
            this.tbPassword.Location = new System.Drawing.Point(57, 299);
            this.tbPassword.Name = "tbPassword";
            this.tbPassword.PasswordChar = '*';
            this.tbPassword.Size = new System.Drawing.Size(260, 20);
            this.tbPassword.TabIndex = 2;
            // 
            // lbPassword
            // 
            this.lbPassword.AutoSize = true;
            this.lbPassword.Location = new System.Drawing.Point(57, 283);
            this.lbPassword.Name = "lbPassword";
            this.lbPassword.Size = new System.Drawing.Size(53, 13);
            this.lbPassword.TabIndex = 1;
            this.lbPassword.Text = "Password";
            // 
            // lbToken
            // 
            this.lbToken.AutoSize = true;
            this.lbToken.Location = new System.Drawing.Point(57, 234);
            this.lbToken.Name = "lbToken";
            this.lbToken.Size = new System.Drawing.Size(38, 13);
            this.lbToken.TabIndex = 1;
            this.lbToken.Text = "Token";
            // 
            // tbToken
            // 
            this.tbToken.Location = new System.Drawing.Point(57, 250);
            this.tbToken.Name = "tbToken";
            this.tbToken.Size = new System.Drawing.Size(260, 20);
            this.tbToken.TabIndex = 2;
            // 
            // lbServerType
            // 
            this.lbServerType.AutoSize = true;
            this.lbServerType.Location = new System.Drawing.Point(57, 167);
            this.lbServerType.Name = "lbServerType";
            this.lbServerType.Size = new System.Drawing.Size(118, 13);
            this.lbServerType.TabIndex = 4;
            this.lbServerType.Text = "TDengine Server Type:";
            // 
            // lbName
            // 
            this.lbName.AutoSize = true;
            this.lbName.Location = new System.Drawing.Point(57, 21);
            this.lbName.Name = "lbName";
            this.lbName.Size = new System.Drawing.Size(122, 13);
            this.lbName.TabIndex = 1;
            this.lbName.Text = "TDengine Server Name:";
            // 
            // tbName
            // 
            this.tbName.Location = new System.Drawing.Point(57, 37);
            this.tbName.Name = "tbName";
            this.tbName.Size = new System.Drawing.Size(260, 20);
            this.tbName.TabIndex = 2;
            // 
            // CreateOrEditTDengineForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(376, 450);
            this.Controls.Add(this.lbServerType);
            this.Controls.Add(this.rbOnCloud);
            this.Controls.Add(this.rbOnPrem);
            this.Controls.Add(this.tbUser);
            this.Controls.Add(this.tbPort);
            this.Controls.Add(this.tbToken);
            this.Controls.Add(this.tbPassword);
            this.Controls.Add(this.tbName);
            this.Controls.Add(this.tbHost);
            this.Controls.Add(this.lbUser);
            this.Controls.Add(this.lbPort);
            this.Controls.Add(this.lbToken);
            this.Controls.Add(this.lbPassword);
            this.Controls.Add(this.lbName);
            this.Controls.Add(this.lbHost);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOK);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "CreateOrEditTDengineForm";
            this.Text = "Create a new TDengine Server";
            this.Load += new System.EventHandler(this.CreditOrEditTDengineForm_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnOK;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Label lbHost;
        private System.Windows.Forms.Label lbPort;
        private System.Windows.Forms.Label lbUser;
        private System.Windows.Forms.TextBox tbHost;
        private System.Windows.Forms.TextBox tbPort;
        private System.Windows.Forms.RadioButton rbOnPrem;
        private System.Windows.Forms.RadioButton rbOnCloud;
        private System.Windows.Forms.TextBox tbUser;
        private System.Windows.Forms.TextBox tbPassword;
        private System.Windows.Forms.Label lbPassword;
        private System.Windows.Forms.Label lbToken;
        private System.Windows.Forms.TextBox tbToken;
        private System.Windows.Forms.Label lbServerType;
        private System.Windows.Forms.Label lbName;
        private System.Windows.Forms.TextBox tbName;
    }
}
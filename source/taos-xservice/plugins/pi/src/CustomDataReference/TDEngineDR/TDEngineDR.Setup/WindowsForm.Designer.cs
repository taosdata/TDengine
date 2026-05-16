
namespace TDEngineDR.Setup
{
    partial class WindowsForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(WindowsForm));
            this.piSystemPicker1 = new OSIsoft.AF.UI.PISystemPicker();
            this.label1 = new System.Windows.Forms.Label();
            this.btnConnect = new System.Windows.Forms.Button();
            this.grAF = new System.Windows.Forms.GroupBox();
            this.lbAccountName = new System.Windows.Forms.Label();
            this.lbVersion = new System.Windows.Forms.Label();
            this.lbName = new System.Windows.Forms.Label();
            this.btnAct = new System.Windows.Forms.Button();
            this.grPlugin = new System.Windows.Forms.GroupBox();
            this.lbPluginVersion = new System.Windows.Forms.Label();
            this.lbPluginName = new System.Windows.Forms.Label();
            this.grAF.SuspendLayout();
            this.grPlugin.SuspendLayout();
            this.SuspendLayout();
            // 
            // piSystemPicker1
            // 
            this.piSystemPicker1.AccessibleDescription = "PI System Picker";
            this.piSystemPicker1.AccessibleName = "PI System Picker";
            this.piSystemPicker1.Cursor = System.Windows.Forms.Cursors.Default;
            this.piSystemPicker1.Location = new System.Drawing.Point(52, 58);
            this.piSystemPicker1.LoginPromptSetting = OSIsoft.AF.UI.PISystemPicker.LoginPromptSettingOptions.Default;
            this.piSystemPicker1.Name = "piSystemPicker1";
            this.piSystemPicker1.ShowBegin = false;
            this.piSystemPicker1.ShowConnect = false;
            this.piSystemPicker1.ShowDelete = false;
            this.piSystemPicker1.ShowEnd = false;
            this.piSystemPicker1.ShowFind = false;
            this.piSystemPicker1.ShowNavigation = false;
            this.piSystemPicker1.ShowNew = false;
            this.piSystemPicker1.ShowNext = false;
            this.piSystemPicker1.ShowPrevious = false;
            this.piSystemPicker1.ShowProperties = false;
            this.piSystemPicker1.Size = new System.Drawing.Size(298, 22);
            this.piSystemPicker1.TabIndex = 0;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(49, 29);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(337, 13);
            this.label1.TabIndex = 1;
            this.label1.Text = "Select which PI System you want to install TDengine Data Reference:";
            // 
            // btnConnect
            // 
            this.btnConnect.Location = new System.Drawing.Point(373, 58);
            this.btnConnect.Name = "btnConnect";
            this.btnConnect.Size = new System.Drawing.Size(75, 23);
            this.btnConnect.TabIndex = 2;
            this.btnConnect.Text = "Connect";
            this.btnConnect.UseVisualStyleBackColor = true;
            this.btnConnect.Click += new System.EventHandler(this.btnConnect_Click);
            // 
            // grAF
            // 
            this.grAF.Controls.Add(this.lbAccountName);
            this.grAF.Controls.Add(this.lbVersion);
            this.grAF.Controls.Add(this.lbName);
            this.grAF.Location = new System.Drawing.Point(52, 110);
            this.grAF.Name = "grAF";
            this.grAF.Size = new System.Drawing.Size(396, 108);
            this.grAF.TabIndex = 3;
            this.grAF.TabStop = false;
            this.grAF.Text = "AF Server Properties";
            // 
            // lbAccountName
            // 
            this.lbAccountName.AutoSize = true;
            this.lbAccountName.Location = new System.Drawing.Point(34, 53);
            this.lbAccountName.Name = "lbAccountName";
            this.lbAccountName.Size = new System.Drawing.Size(81, 13);
            this.lbAccountName.TabIndex = 0;
            this.lbAccountName.Text = "Account Name:";
            // 
            // lbVersion
            // 
            this.lbVersion.AutoSize = true;
            this.lbVersion.Location = new System.Drawing.Point(34, 86);
            this.lbVersion.Name = "lbVersion";
            this.lbVersion.Size = new System.Drawing.Size(48, 13);
            this.lbVersion.TabIndex = 0;
            this.lbVersion.Text = "Version: ";
            // 
            // lbName
            // 
            this.lbName.AutoSize = true;
            this.lbName.Location = new System.Drawing.Point(34, 26);
            this.lbName.Name = "lbName";
            this.lbName.Size = new System.Drawing.Size(41, 13);
            this.lbName.TabIndex = 0;
            this.lbName.Text = "Name: ";
            // 
            // btnAct
            // 
            this.btnAct.Location = new System.Drawing.Point(200, 397);
            this.btnAct.Name = "btnAct";
            this.btnAct.Size = new System.Drawing.Size(75, 23);
            this.btnAct.TabIndex = 4;
            this.btnAct.Text = "Install";
            this.btnAct.UseVisualStyleBackColor = true;
            this.btnAct.Click += new System.EventHandler(this.btnAct_Click);
            // 
            // grPlugin
            // 
            this.grPlugin.Controls.Add(this.lbPluginVersion);
            this.grPlugin.Controls.Add(this.lbPluginName);
            this.grPlugin.Location = new System.Drawing.Point(52, 233);
            this.grPlugin.Name = "grPlugin";
            this.grPlugin.Size = new System.Drawing.Size(396, 127);
            this.grPlugin.TabIndex = 5;
            this.grPlugin.TabStop = false;
            this.grPlugin.Text = "TDengine Plugin";
            // 
            // lbPluginVersion
            // 
            this.lbPluginVersion.AutoSize = true;
            this.lbPluginVersion.Location = new System.Drawing.Point(34, 69);
            this.lbPluginVersion.Name = "lbPluginVersion";
            this.lbPluginVersion.Size = new System.Drawing.Size(48, 13);
            this.lbPluginVersion.TabIndex = 0;
            this.lbPluginVersion.Text = "Version: ";
            // 
            // lbPluginName
            // 
            this.lbPluginName.AutoSize = true;
            this.lbPluginName.Location = new System.Drawing.Point(34, 37);
            this.lbPluginName.Name = "lbPluginName";
            this.lbPluginName.Size = new System.Drawing.Size(41, 13);
            this.lbPluginName.TabIndex = 0;
            this.lbPluginName.Text = "Name: ";
            // 
            // WindowsForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(493, 450);
            this.Controls.Add(this.grPlugin);
            this.Controls.Add(this.btnAct);
            this.Controls.Add(this.grAF);
            this.Controls.Add(this.btnConnect);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.piSystemPicker1);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "WindowsForm";
            this.Text = "TDengine PI Custom Data Reference Installer";
            this.grAF.ResumeLayout(false);
            this.grAF.PerformLayout();
            this.grPlugin.ResumeLayout(false);
            this.grPlugin.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private OSIsoft.AF.UI.PISystemPicker piSystemPicker1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnConnect;
        private System.Windows.Forms.GroupBox grAF;
        private System.Windows.Forms.Button btnAct;
        private System.Windows.Forms.Label lbVersion;
        private System.Windows.Forms.Label lbName;
        private System.Windows.Forms.Label lbAccountName;
        private System.Windows.Forms.GroupBox grPlugin;
        private System.Windows.Forms.Label lbPluginVersion;
        private System.Windows.Forms.Label lbPluginName;
    }
}


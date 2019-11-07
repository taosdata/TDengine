namespace excel2010
{
    partial class TDAboutForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDAboutForm));
            this.Input = new System.Windows.Forms.GroupBox();
            this.emailLabel = new System.Windows.Forms.Label();
            this.clientVersionLabel = new System.Windows.Forms.Label();
            this.serverVersionLable = new System.Windows.Forms.Label();
            this.Input.SuspendLayout();
            this.SuspendLayout();
            // 
            // Input
            // 
            this.Input.Controls.Add(this.emailLabel);
            this.Input.Controls.Add(this.clientVersionLabel);
            this.Input.Controls.Add(this.serverVersionLable);
            this.Input.Location = new System.Drawing.Point(15, 10);
            this.Input.Margin = new System.Windows.Forms.Padding(3, 0, 3, 0);
            this.Input.Name = "Input";
            this.Input.Padding = new System.Windows.Forms.Padding(3, 0, 3, 0);
            this.Input.Size = new System.Drawing.Size(320, 125);
            this.Input.TabIndex = 1;
            this.Input.TabStop = false;
            // 
            // emailLabel
            // 
            this.emailLabel.AutoSize = true;
            this.emailLabel.Location = new System.Drawing.Point(15, 85);
            this.emailLabel.Name = "emailLabel";
            this.emailLabel.Size = new System.Drawing.Size(224, 18);
            this.emailLabel.TabIndex = 4;
            this.emailLabel.Text = "Email: support@taosdata.com";
            // 
            // clientVersionLabel
            // 
            this.clientVersionLabel.AutoSize = true;
            this.clientVersionLabel.Location = new System.Drawing.Point(15, 55);
            this.clientVersionLabel.Name = "clientVersionLabel";
            this.clientVersionLabel.Size = new System.Drawing.Size(160, 18);
            this.clientVersionLabel.TabIndex = 2;
            this.clientVersionLabel.Text = "Plugin Version: 1.0";
            // 
            // serverVersionLable
            // 
            this.serverVersionLable.AutoSize = true;
            this.serverVersionLable.Location = new System.Drawing.Point(15, 25);
            this.serverVersionLable.Name = "serverVersionLable";
            this.serverVersionLable.Size = new System.Drawing.Size(192, 18);
            this.serverVersionLable.TabIndex = 0;
            this.serverVersionLable.Text = "TDengine Version: 1.*.*";
            // 
            // TDAboutForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(350, 147);
            this.Controls.Add(this.Input);
            this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Margin = new System.Windows.Forms.Padding(3, 0, 3, 0);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TDAboutForm";
            this.Text = "About this Plugin";
            this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.Form_KeyPress);
            this.Input.ResumeLayout(false);
            this.Input.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox Input;
        private System.Windows.Forms.Label emailLabel;
        private System.Windows.Forms.Label clientVersionLabel;
        private System.Windows.Forms.Label serverVersionLable;
    }
}
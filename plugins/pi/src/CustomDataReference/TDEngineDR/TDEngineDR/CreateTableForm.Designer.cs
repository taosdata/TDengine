
namespace TDEngineDR
{
    partial class CreateTableForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(CreateTableForm));
            this.lbServer = new System.Windows.Forms.Label();
            this.lbPointName = new System.Windows.Forms.Label();
            this.lbPointType = new System.Windows.Forms.Label();
            this.cbServer = new System.Windows.Forms.ComboBox();
            this.cbPointType = new System.Windows.Forms.ComboBox();
            this.tbPointName = new System.Windows.Forms.TextBox();
            this.btnOk = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.lbDatabase = new System.Windows.Forms.Label();
            this.tbDatabase = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // lbServer
            // 
            this.lbServer.AutoSize = true;
            this.lbServer.Location = new System.Drawing.Point(32, 40);
            this.lbServer.Name = "lbServer";
            this.lbServer.Size = new System.Drawing.Size(88, 13);
            this.lbServer.TabIndex = 0;
            this.lbServer.Text = "TDengine Server";
            // 
            // lbPointName
            // 
            this.lbPointName.AutoSize = true;
            this.lbPointName.Location = new System.Drawing.Point(32, 127);
            this.lbPointName.Name = "lbPointName";
            this.lbPointName.Size = new System.Drawing.Size(65, 13);
            this.lbPointName.TabIndex = 1;
            this.lbPointName.Text = "Point Name:";
            // 
            // lbPointType
            // 
            this.lbPointType.AutoSize = true;
            this.lbPointType.Location = new System.Drawing.Point(36, 181);
            this.lbPointType.Name = "lbPointType";
            this.lbPointType.Size = new System.Drawing.Size(61, 13);
            this.lbPointType.TabIndex = 2;
            this.lbPointType.Text = "Point Type:";
            // 
            // cbServer
            // 
            this.cbServer.FormattingEnabled = true;
            this.cbServer.Location = new System.Drawing.Point(35, 56);
            this.cbServer.Name = "cbServer";
            this.cbServer.Size = new System.Drawing.Size(257, 21);
            this.cbServer.TabIndex = 3;
            // 
            // cbPointType
            // 
            this.cbPointType.FormattingEnabled = true;
            this.cbPointType.Location = new System.Drawing.Point(35, 197);
            this.cbPointType.Name = "cbPointType";
            this.cbPointType.Size = new System.Drawing.Size(257, 21);
            this.cbPointType.TabIndex = 4;
            // 
            // tbPointName
            // 
            this.tbPointName.Location = new System.Drawing.Point(35, 143);
            this.tbPointName.Name = "tbPointName";
            this.tbPointName.Size = new System.Drawing.Size(257, 20);
            this.tbPointName.TabIndex = 5;
            // 
            // btnOk
            // 
            this.btnOk.Location = new System.Drawing.Point(35, 233);
            this.btnOk.Name = "btnOk";
            this.btnOk.Size = new System.Drawing.Size(99, 23);
            this.btnOk.TabIndex = 6;
            this.btnOk.Text = "Create Table";
            this.btnOk.UseVisualStyleBackColor = true;
            this.btnOk.Click += new System.EventHandler(this.btnOK_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(217, 233);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 7;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(32, 18);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(272, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "Please complete the fields to create the TDengine table:";
            // 
            // lbDatabase
            // 
            this.lbDatabase.AutoSize = true;
            this.lbDatabase.Location = new System.Drawing.Point(32, 80);
            this.lbDatabase.Name = "lbDatabase";
            this.lbDatabase.Size = new System.Drawing.Size(56, 13);
            this.lbDatabase.TabIndex = 1;
            this.lbDatabase.Text = "Database:";
            // 
            // tbDatabase
            // 
            this.tbDatabase.Location = new System.Drawing.Point(35, 96);
            this.tbDatabase.Name = "tbDatabase";
            this.tbDatabase.Size = new System.Drawing.Size(257, 20);
            this.tbDatabase.TabIndex = 5;
            // 
            // CreateTableForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(330, 282);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOk);
            this.Controls.Add(this.tbDatabase);
            this.Controls.Add(this.tbPointName);
            this.Controls.Add(this.cbPointType);
            this.Controls.Add(this.cbServer);
            this.Controls.Add(this.lbPointType);
            this.Controls.Add(this.lbDatabase);
            this.Controls.Add(this.lbPointName);
            this.Controls.Add(this.lbServer);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "CreateTableForm";
            this.Text = "Create TDengine Table/PI Point";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbServer;
        private System.Windows.Forms.Label lbPointName;
        private System.Windows.Forms.Label lbPointType;
        private System.Windows.Forms.ComboBox cbServer;
        private System.Windows.Forms.ComboBox cbPointType;
        private System.Windows.Forms.TextBox tbPointName;
        private System.Windows.Forms.Button btnOk;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label lbDatabase;
        private System.Windows.Forms.TextBox tbDatabase;
    }
}
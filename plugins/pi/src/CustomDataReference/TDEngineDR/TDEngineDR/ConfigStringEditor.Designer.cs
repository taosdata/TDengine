
namespace TDEngineDR
{
    partial class ConfigStringEditor
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ConfigStringEditor));
            this.lbIntro = new System.Windows.Forms.Label();
            this.lbServer = new System.Windows.Forms.Label();
            this.lbPoint = new System.Windows.Forms.Label();
            this.tbPoint = new System.Windows.Forms.TextBox();
            this.btnOK = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.cbServer = new System.Windows.Forms.ComboBox();
            this.btnCreateTDEngine = new System.Windows.Forms.Button();
            this.rbPIPoint = new System.Windows.Forms.RadioButton();
            this.rbAFElement = new System.Windows.Forms.RadioButton();
            this.tbTable = new System.Windows.Forms.TextBox();
            this.lbTable = new System.Windows.Forms.Label();
            this.tbColumn = new System.Windows.Forms.TextBox();
            this.lbColumn = new System.Windows.Forms.Label();
            this.lbDatabase = new System.Windows.Forms.Label();
            this.tbDatabase = new System.Windows.Forms.TextBox();
            this.rbTable = new System.Windows.Forms.RadioButton();
            this.SuspendLayout();
            // 
            // lbIntro
            // 
            this.lbIntro.AutoSize = true;
            this.lbIntro.Location = new System.Drawing.Point(48, 9);
            this.lbIntro.Name = "lbIntro";
            this.lbIntro.Size = new System.Drawing.Size(257, 13);
            this.lbIntro.TabIndex = 0;
            this.lbIntro.Text = "Please type the TDengine server and stream settings:";
            // 
            // lbServer
            // 
            this.lbServer.AutoSize = true;
            this.lbServer.Location = new System.Drawing.Point(48, 47);
            this.lbServer.Name = "lbServer";
            this.lbServer.Size = new System.Drawing.Size(41, 13);
            this.lbServer.TabIndex = 0;
            this.lbServer.Text = "Server:";
            // 
            // lbPoint
            // 
            this.lbPoint.AutoSize = true;
            this.lbPoint.Location = new System.Drawing.Point(48, 139);
            this.lbPoint.Name = "lbPoint";
            this.lbPoint.Size = new System.Drawing.Size(47, 13);
            this.lbPoint.TabIndex = 0;
            this.lbPoint.Text = "PI Point:";
            // 
            // tbPoint
            // 
            this.tbPoint.Location = new System.Drawing.Point(140, 136);
            this.tbPoint.Name = "tbPoint";
            this.tbPoint.Size = new System.Drawing.Size(181, 20);
            this.tbPoint.TabIndex = 1;
            // 
            // btnOK
            // 
            this.btnOK.Location = new System.Drawing.Point(88, 246);
            this.btnOK.Name = "btnOK";
            this.btnOK.Size = new System.Drawing.Size(75, 23);
            this.btnOK.TabIndex = 2;
            this.btnOK.Text = "OK";
            this.btnOK.UseVisualStyleBackColor = true;
            this.btnOK.Click += new System.EventHandler(this.btnOK_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(208, 246);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 2;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // cbServer
            // 
            this.cbServer.FormattingEnabled = true;
            this.cbServer.Location = new System.Drawing.Point(140, 44);
            this.cbServer.Name = "cbServer";
            this.cbServer.Size = new System.Drawing.Size(181, 21);
            this.cbServer.TabIndex = 3;
            // 
            // btnCreateTDEngine
            // 
            this.btnCreateTDEngine.Location = new System.Drawing.Point(208, 190);
            this.btnCreateTDEngine.Name = "btnCreateTDEngine";
            this.btnCreateTDEngine.Size = new System.Drawing.Size(113, 23);
            this.btnCreateTDEngine.TabIndex = 4;
            this.btnCreateTDEngine.Text = "Create table";
            this.btnCreateTDEngine.UseVisualStyleBackColor = true;
            this.btnCreateTDEngine.Click += new System.EventHandler(this.btnCreateTDEngine_Click);
            // 
            // rbPIPoint
            // 
            this.rbPIPoint.AutoSize = true;
            this.rbPIPoint.Location = new System.Drawing.Point(50, 97);
            this.rbPIPoint.Name = "rbPIPoint";
            this.rbPIPoint.Size = new System.Drawing.Size(62, 17);
            this.rbPIPoint.TabIndex = 5;
            this.rbPIPoint.TabStop = true;
            this.rbPIPoint.Text = "PI Point";
            this.rbPIPoint.UseVisualStyleBackColor = true;
            this.rbPIPoint.CheckedChanged += new System.EventHandler(this.rbPIPoint_CheckedChanged);
            // 
            // rbAFElement
            // 
            this.rbAFElement.AutoSize = true;
            this.rbAFElement.Location = new System.Drawing.Point(150, 97);
            this.rbAFElement.Name = "rbAFElement";
            this.rbAFElement.Size = new System.Drawing.Size(79, 17);
            this.rbAFElement.TabIndex = 5;
            this.rbAFElement.TabStop = true;
            this.rbAFElement.Text = "AF Element";
            this.rbAFElement.TextAlign = System.Drawing.ContentAlignment.BottomLeft;
            this.rbAFElement.UseVisualStyleBackColor = true;
            this.rbAFElement.CheckedChanged += new System.EventHandler(this.rbAFAttribute_CheckedChanged);
            // 
            // tbTable
            // 
            this.tbTable.Location = new System.Drawing.Point(140, 138);
            this.tbTable.Name = "tbTable";
            this.tbTable.Size = new System.Drawing.Size(181, 20);
            this.tbTable.TabIndex = 1;
            // 
            // lbTable
            // 
            this.lbTable.AutoSize = true;
            this.lbTable.Location = new System.Drawing.Point(48, 139);
            this.lbTable.Name = "lbTable";
            this.lbTable.Size = new System.Drawing.Size(37, 13);
            this.lbTable.TabIndex = 0;
            this.lbTable.Text = "Table:";
            // 
            // tbColumn
            // 
            this.tbColumn.Location = new System.Drawing.Point(140, 164);
            this.tbColumn.Name = "tbColumn";
            this.tbColumn.Size = new System.Drawing.Size(181, 20);
            this.tbColumn.TabIndex = 1;
            // 
            // lbColumn
            // 
            this.lbColumn.AutoSize = true;
            this.lbColumn.Location = new System.Drawing.Point(48, 167);
            this.lbColumn.Name = "lbColumn";
            this.lbColumn.Size = new System.Drawing.Size(45, 13);
            this.lbColumn.TabIndex = 0;
            this.lbColumn.Text = "Column:";
            // 
            // lbDatabase
            // 
            this.lbDatabase.AutoSize = true;
            this.lbDatabase.Location = new System.Drawing.Point(48, 74);
            this.lbDatabase.Name = "lbDatabase";
            this.lbDatabase.Size = new System.Drawing.Size(56, 13);
            this.lbDatabase.TabIndex = 0;
            this.lbDatabase.Text = "Database:";
            // 
            // tbDatabase
            // 
            this.tbDatabase.Location = new System.Drawing.Point(140, 71);
            this.tbDatabase.Name = "tbDatabase";
            this.tbDatabase.Size = new System.Drawing.Size(181, 20);
            this.tbDatabase.TabIndex = 1;
            // 
            // rbTable
            // 
            this.rbTable.AutoSize = true;
            this.rbTable.Location = new System.Drawing.Point(254, 97);
            this.rbTable.Name = "rbTable";
            this.rbTable.Size = new System.Drawing.Size(52, 17);
            this.rbTable.TabIndex = 5;
            this.rbTable.TabStop = true;
            this.rbTable.Text = "Table";
            this.rbTable.TextAlign = System.Drawing.ContentAlignment.BottomLeft;
            this.rbTable.UseVisualStyleBackColor = true;
            this.rbTable.CheckedChanged += new System.EventHandler(this.rbTable_CheckedChanged);
            // 
            // ConfigStringEditor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(360, 296);
            this.Controls.Add(this.rbTable);
            this.Controls.Add(this.rbAFElement);
            this.Controls.Add(this.rbPIPoint);
            this.Controls.Add(this.btnCreateTDEngine);
            this.Controls.Add(this.cbServer);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOK);
            this.Controls.Add(this.tbDatabase);
            this.Controls.Add(this.tbTable);
            this.Controls.Add(this.lbTable);
            this.Controls.Add(this.tbColumn);
            this.Controls.Add(this.tbPoint);
            this.Controls.Add(this.lbColumn);
            this.Controls.Add(this.lbPoint);
            this.Controls.Add(this.lbDatabase);
            this.Controls.Add(this.lbServer);
            this.Controls.Add(this.lbIntro);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "ConfigStringEditor";
            this.Text = "TDengine Stream Settings";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbIntro;
        private System.Windows.Forms.Label lbServer;
        private System.Windows.Forms.Label lbPoint;
        private System.Windows.Forms.Button btnOK;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Button btnCreateTDEngine;
        public System.Windows.Forms.ComboBox cbServer;
        public System.Windows.Forms.TextBox tbPoint;
        private System.Windows.Forms.RadioButton rbPIPoint;
        private System.Windows.Forms.RadioButton rbAFElement;
        public System.Windows.Forms.TextBox tbTable;
        private System.Windows.Forms.Label lbTable;
        public System.Windows.Forms.TextBox tbColumn;
        private System.Windows.Forms.Label lbColumn;
        private System.Windows.Forms.Label lbDatabase;
        public System.Windows.Forms.TextBox tbDatabase;
        private System.Windows.Forms.RadioButton rbTable;
    }
}
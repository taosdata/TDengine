namespace excel2010
{
    partial class TDTablesForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDTablesForm));
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.filterTableCheckBox = new System.Windows.Forms.CheckBox();
            this.tablenameTextBox = new System.Windows.Forms.TextBox();
            this.tagValuesCheck = new System.Windows.Forms.CheckBox();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.basicinfoCheck = new System.Windows.Forms.CheckBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.import = new System.Windows.Forms.Button();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.inputButton = new System.Windows.Forms.Button();
            this.inputTextBox = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.filterTableCheckBox);
            this.groupBox1.Controls.Add(this.tablenameTextBox);
            this.groupBox1.Controls.Add(this.tagValuesCheck);
            this.groupBox1.Controls.Add(this.headsCheck);
            this.groupBox1.Controls.Add(this.basicinfoCheck);
            this.groupBox1.Location = new System.Drawing.Point(15, 85);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox1.Size = new System.Drawing.Size(480, 155);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Options";
            // 
            // filterTableCheckBox
            // 
            this.filterTableCheckBox.AutoSize = true;
            this.filterTableCheckBox.Location = new System.Drawing.Point(15, 115);
            this.filterTableCheckBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.filterTableCheckBox.Name = "filterTableCheckBox";
            this.filterTableCheckBox.Size = new System.Drawing.Size(195, 22);
            this.filterTableCheckBox.TabIndex = 4;
            this.filterTableCheckBox.Text = "filter table (use %_)";
            this.filterTableCheckBox.UseVisualStyleBackColor = true;
            this.filterTableCheckBox.CheckedChanged += new System.EventHandler(this.FilterTableCheckBox_CheckedChanged);
            // 
            // tablenameTextBox
            // 
            this.tablenameTextBox.Location = new System.Drawing.Point(229, 114);
            this.tablenameTextBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.tablenameTextBox.Name = "tablenameTextBox";
            this.tablenameTextBox.Size = new System.Drawing.Size(228, 25);
            this.tablenameTextBox.TabIndex = 1;
            this.tablenameTextBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.InputTextbox_MouseClick);
            // 
            // tagValuesCheck
            // 
            this.tagValuesCheck.AutoSize = true;
            this.tagValuesCheck.Location = new System.Drawing.Point(15, 85);
            this.tagValuesCheck.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.tagValuesCheck.Name = "tagValuesCheck";
            this.tagValuesCheck.Size = new System.Drawing.Size(147, 22);
            this.tagValuesCheck.TabIndex = 4;
            this.tagValuesCheck.Text = "show tag Values";
            this.tagValuesCheck.UseVisualStyleBackColor = true;
            this.tagValuesCheck.CheckedChanged += new System.EventHandler(this.TagValuesCheck_CheckedChanged);
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Location = new System.Drawing.Point(15, 25);
            this.headsCheck.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.headsCheck.Name = "headsCheck";
            this.headsCheck.Size = new System.Drawing.Size(107, 22);
            this.headsCheck.TabIndex = 0;
            this.headsCheck.Text = "show heads";
            this.headsCheck.UseVisualStyleBackColor = true;
            // 
            // basicinfoCheck
            // 
            this.basicinfoCheck.AutoSize = true;
            this.basicinfoCheck.Location = new System.Drawing.Point(15, 55);
            this.basicinfoCheck.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.basicinfoCheck.Name = "basicinfoCheck";
            this.basicinfoCheck.Size = new System.Drawing.Size(203, 22);
            this.basicinfoCheck.TabIndex = 1;
            this.basicinfoCheck.Text = "show basic information";
            this.basicinfoCheck.UseVisualStyleBackColor = true;
            this.basicinfoCheck.CheckedChanged += new System.EventHandler(this.BasicinfoCheck_CheckedChanged);
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.outputButton);
            this.groupBox2.Controls.Add(this.outputTextbox);
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Location = new System.Drawing.Point(15, 250);
            this.groupBox2.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox2.Size = new System.Drawing.Size(480, 65);
            this.groupBox2.TabIndex = 2;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Output";
            // 
            // outputButton
            // 
            this.outputButton.Image = ((System.Drawing.Image)(resources.GetObject("outputButton.Image")));
            this.outputButton.Location = new System.Drawing.Point(432, 24);
            this.outputButton.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.outputButton.Name = "outputButton";
            this.outputButton.Size = new System.Drawing.Size(26, 26);
            this.outputButton.TabIndex = 2;
            this.outputButton.UseVisualStyleBackColor = true;
            this.outputButton.Click += new System.EventHandler(this.OutputButton_Click);
            // 
            // outputTextbox
            // 
            this.outputTextbox.Location = new System.Drawing.Point(229, 24);
            this.outputTextbox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.outputTextbox.Name = "outputTextbox";
            this.outputTextbox.Size = new System.Drawing.Size(196, 25);
            this.outputTextbox.TabIndex = 1;
            this.outputTextbox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.OutputTextbox_MouseClick);
            this.outputTextbox.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.OutputTextbox_KeyPress);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(15, 25);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(192, 18);
            this.label1.TabIndex = 0;
            this.label1.Text = "select start cell (1*1)";
            // 
            // import
            // 
            this.import.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.import.Location = new System.Drawing.Point(425, 325);
            this.import.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(70, 29);
            this.import.TabIndex = 3;
            this.import.Text = "Import";
            this.import.UseVisualStyleBackColor = true;
            this.import.Click += new System.EventHandler(this.Import_Click);
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.inputButton);
            this.groupBox3.Controls.Add(this.inputTextBox);
            this.groupBox3.Controls.Add(this.label2);
            this.groupBox3.Location = new System.Drawing.Point(15, 10);
            this.groupBox3.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox3.Size = new System.Drawing.Size(480, 65);
            this.groupBox3.TabIndex = 0;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Input";
            // 
            // inputButton
            // 
            this.inputButton.Image = ((System.Drawing.Image)(resources.GetObject("inputButton.Image")));
            this.inputButton.Location = new System.Drawing.Point(432, 24);
            this.inputButton.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.inputButton.Name = "inputButton";
            this.inputButton.Size = new System.Drawing.Size(26, 26);
            this.inputButton.TabIndex = 2;
            this.inputButton.UseVisualStyleBackColor = true;
            this.inputButton.Click += new System.EventHandler(this.InputSelect_Click);
            // 
            // inputTextBox
            // 
            this.inputTextBox.Location = new System.Drawing.Point(229, 24);
            this.inputTextBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(196, 25);
            this.inputTextBox.TabIndex = 1;
            this.inputTextBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.InputTextbox_MouseClick);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(15, 25);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(184, 18);
            this.label2.TabIndex = 0;
            this.label2.Text = "super table name (1*1)";
            // 
            // TDTablesForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(509, 366);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.import);
            this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.KeyPreview = true;
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TDTablesForm";
            this.Text = "Query Tables";
            this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.Form_KeyPress);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.CheckBox headsCheck;
        private System.Windows.Forms.CheckBox basicinfoCheck;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button outputButton;
        private System.Windows.Forms.TextBox outputTextbox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button import;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.Button inputButton;
        private System.Windows.Forms.TextBox inputTextBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.CheckBox tagValuesCheck;
        private System.Windows.Forms.CheckBox filterTableCheckBox;
        private System.Windows.Forms.TextBox tablenameTextBox;
    }
}
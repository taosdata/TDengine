namespace excel2010
{
    partial class TDStablesForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDStablesForm));
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.basicinfoCheck = new System.Windows.Forms.CheckBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.import = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.headsCheck);
            this.groupBox1.Controls.Add(this.basicinfoCheck);
            this.groupBox1.Location = new System.Drawing.Point(15, 10);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox1.Size = new System.Drawing.Size(480, 90);
            this.groupBox1.TabIndex = 0;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Options";
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
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.outputButton);
            this.groupBox2.Controls.Add(this.outputTextbox);
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Location = new System.Drawing.Point(15, 110);
            this.groupBox2.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.groupBox2.Size = new System.Drawing.Size(480, 65);
            this.groupBox2.TabIndex = 1;
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
            this.label1.Text = "select start cell (1x1)";
            // 
            // import
            // 
            this.import.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.import.Location = new System.Drawing.Point(425, 185);
            this.import.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(70, 29);
            this.import.TabIndex = 2;
            this.import.Text = "Import";
            this.import.UseVisualStyleBackColor = true;
            this.import.Click += new System.EventHandler(this.Import_Click);
            // 
            // TDStablesForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(509, 226);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.import);
            this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.KeyPreview = true;
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TDStablesForm";
            this.Text = "Query STables";
            this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.Form_KeyPress);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.CheckBox basicinfoCheck;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.TextBox outputTextbox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button import;
        private System.Windows.Forms.Button outputButton;
        private System.Windows.Forms.CheckBox headsCheck;
    }
}
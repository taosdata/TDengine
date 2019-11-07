namespace excel2010
{
    partial class TDSliceForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDSliceForm));
            this.rawListView = new System.Windows.Forms.ListView();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.timestampCheck = new System.Windows.Forms.CheckBox();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
            this.inputButton = new System.Windows.Forms.Button();
            this.inputTextBox = new System.Windows.Forms.TextBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.fillMethodCombox = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.import = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.SuspendLayout();
            // 
            // rawListView
            // 
            this.rawListView.Location = new System.Drawing.Point(15, 215);
            this.rawListView.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(479, 180);
            this.rawListView.TabIndex = 2;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.timestampCheck);
            this.groupBox1.Controls.Add(this.headsCheck);
            this.groupBox1.Location = new System.Drawing.Point(15, 145);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Size = new System.Drawing.Size(480, 60);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Options";
            // 
            // timestampCheck
            // 
            this.timestampCheck.AutoSize = true;
            this.timestampCheck.Location = new System.Drawing.Point(229, 25);
            this.timestampCheck.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.timestampCheck.Name = "timestampCheck";
            this.timestampCheck.Size = new System.Drawing.Size(187, 22);
            this.timestampCheck.TabIndex = 1;
            this.timestampCheck.Text = "display as timestamp";
            this.timestampCheck.UseVisualStyleBackColor = true;
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Location = new System.Drawing.Point(15, 25);
            this.headsCheck.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.headsCheck.Name = "headsCheck";
            this.headsCheck.Size = new System.Drawing.Size(107, 22);
            this.headsCheck.TabIndex = 0;
            this.headsCheck.Text = "show heads";
            this.headsCheck.UseVisualStyleBackColor = true;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(18, 85);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(168, 18);
            this.label4.TabIndex = 5;
            this.label4.Text = "interpolation method";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(15, 55);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(80, 18);
            this.label3.TabIndex = 3;
            this.label3.Text = "timestamp";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.outputButton);
            this.groupBox2.Controls.Add(this.outputTextbox);
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Location = new System.Drawing.Point(15, 405);
            this.groupBox2.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox2.Size = new System.Drawing.Size(480, 65);
            this.groupBox2.TabIndex = 3;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Output";
            // 
            // outputButton
            // 
            this.outputButton.Image = ((System.Drawing.Image)(resources.GetObject("outputButton.Image")));
            this.outputButton.Location = new System.Drawing.Point(432, 24);
            this.outputButton.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.outputButton.Name = "outputButton";
            this.outputButton.Size = new System.Drawing.Size(26, 26);
            this.outputButton.TabIndex = 2;
            this.outputButton.UseVisualStyleBackColor = true;
            this.outputButton.Click += new System.EventHandler(this.OutputButton_Click);
            // 
            // outputTextbox
            // 
            this.outputTextbox.Location = new System.Drawing.Point(229, 24);
            this.outputTextbox.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
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
            // fromTimePicker
            // 
            this.fromTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.fromTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.fromTimePicker.Location = new System.Drawing.Point(229, 54);
            this.fromTimePicker.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.fromTimePicker.Name = "fromTimePicker";
            this.fromTimePicker.Size = new System.Drawing.Size(228, 25);
            this.fromTimePicker.TabIndex = 4;
            // 
            // inputButton
            // 
            this.inputButton.Image = ((System.Drawing.Image)(resources.GetObject("inputButton.Image")));
            this.inputButton.Location = new System.Drawing.Point(432, 24);
            this.inputButton.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.inputButton.Name = "inputButton";
            this.inputButton.Size = new System.Drawing.Size(26, 26);
            this.inputButton.TabIndex = 2;
            this.inputButton.UseVisualStyleBackColor = true;
            this.inputButton.Click += new System.EventHandler(this.InputButton_Click);
            // 
            // inputTextBox
            // 
            this.inputTextBox.Location = new System.Drawing.Point(229, 24);
            this.inputTextBox.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.inputTextBox.MaxLength = 64000;
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(196, 25);
            this.inputTextBox.TabIndex = 1;
            this.inputTextBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.InputTextbox_MouseClick);
            this.inputTextBox.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.InputTextbox_KeyPress);
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.fillMethodCombox);
            this.groupBox3.Controls.Add(this.fromTimePicker);
            this.groupBox3.Controls.Add(this.inputButton);
            this.groupBox3.Controls.Add(this.inputTextBox);
            this.groupBox3.Controls.Add(this.label4);
            this.groupBox3.Controls.Add(this.label3);
            this.groupBox3.Controls.Add(this.label2);
            this.groupBox3.Location = new System.Drawing.Point(15, 10);
            this.groupBox3.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Size = new System.Drawing.Size(480, 125);
            this.groupBox3.TabIndex = 0;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Input";
            // 
            // fillMethodCombox
            // 
            this.fillMethodCombox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.fillMethodCombox.FormattingEnabled = true;
            this.fillMethodCombox.Location = new System.Drawing.Point(229, 84);
            this.fillMethodCombox.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.fillMethodCombox.Name = "fillMethodCombox";
            this.fillMethodCombox.Size = new System.Drawing.Size(228, 26);
            this.fillMethodCombox.TabIndex = 6;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(15, 25);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(200, 18);
            this.label2.TabIndex = 0;
            this.label2.Text = "name of tables or stable";
            // 
            // import
            // 
            this.import.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.import.Location = new System.Drawing.Point(425, 480);
            this.import.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(70, 29);
            this.import.TabIndex = 4;
            this.import.Text = "Import";
            this.import.UseVisualStyleBackColor = true;
            this.import.Click += new System.EventHandler(this.Import_Click);
            // 
            // TDSliceForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(509, 521);
            this.Controls.Add(this.rawListView);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.import);
            this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.KeyPreview = true;
            this.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Name = "TDSliceForm";
            this.Text = "Query Slice Data";
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

        private System.Windows.Forms.ListView rawListView;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.CheckBox timestampCheck;
        private System.Windows.Forms.CheckBox headsCheck;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button outputButton;
        private System.Windows.Forms.TextBox outputTextbox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.DateTimePicker fromTimePicker;
        private System.Windows.Forms.Button inputButton;
        private System.Windows.Forms.TextBox inputTextBox;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button import;
        private System.Windows.Forms.ComboBox fillMethodCombox;
    }
}
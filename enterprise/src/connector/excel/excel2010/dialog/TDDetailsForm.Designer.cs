namespace excel2010
{
    partial class TDDetailsForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDDetailsForm));
            this.rawListView = new System.Windows.Forms.ListView();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.ascCheck = new System.Windows.Forms.CheckBox();
            this.timestampCheck = new System.Windows.Forms.CheckBox();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.limitrowsNumericUpDown = new System.Windows.Forms.NumericUpDown();
            this.toTimePicker = new System.Windows.Forms.DateTimePicker();
            this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
            this.inputButton = new System.Windows.Forms.Button();
            this.inputTextBox = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.import = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.limitrowsNumericUpDown)).BeginInit();
            this.SuspendLayout();
            // 
            // rawListView
            // 
            this.rawListView.Location = new System.Drawing.Point(15, 280);
            this.rawListView.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(480, 180);
            this.rawListView.TabIndex = 2;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.ascCheck);
            this.groupBox1.Controls.Add(this.timestampCheck);
            this.groupBox1.Controls.Add(this.headsCheck);
            this.groupBox1.Location = new System.Drawing.Point(15, 175);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Size = new System.Drawing.Size(480, 95);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Options";
            // 
            // ascCheck
            // 
            this.ascCheck.AutoSize = true;
            this.ascCheck.Location = new System.Drawing.Point(18, 55);
            this.ascCheck.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.ascCheck.Name = "ascCheck";
            this.ascCheck.Size = new System.Drawing.Size(163, 22);
            this.ascCheck.TabIndex = 2;
            this.ascCheck.Text = "ascending by time";
            this.ascCheck.UseVisualStyleBackColor = true;
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
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.outputButton);
            this.groupBox2.Controls.Add(this.outputTextbox);
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Location = new System.Drawing.Point(15, 470);
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
            this.label1.Text = "select start cell (1*1)";
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.limitrowsNumericUpDown);
            this.groupBox3.Controls.Add(this.toTimePicker);
            this.groupBox3.Controls.Add(this.fromTimePicker);
            this.groupBox3.Controls.Add(this.inputButton);
            this.groupBox3.Controls.Add(this.inputTextBox);
            this.groupBox3.Controls.Add(this.label5);
            this.groupBox3.Controls.Add(this.label4);
            this.groupBox3.Controls.Add(this.label3);
            this.groupBox3.Controls.Add(this.label2);
            this.groupBox3.Location = new System.Drawing.Point(15, 10);
            this.groupBox3.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Size = new System.Drawing.Size(480, 155);
            this.groupBox3.TabIndex = 0;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Input";
            // 
            // limitrowsNumericUpDown
            // 
            this.limitrowsNumericUpDown.Increment = new decimal(new int[] {
            100,
            0,
            0,
            0});
            this.limitrowsNumericUpDown.Location = new System.Drawing.Point(229, 114);
            this.limitrowsNumericUpDown.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.limitrowsNumericUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.limitrowsNumericUpDown.Name = "limitrowsNumericUpDown";
            this.limitrowsNumericUpDown.Size = new System.Drawing.Size(229, 25);
            this.limitrowsNumericUpDown.TabIndex = 8;
            // 
            // toTimePicker
            // 
            this.toTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.toTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.toTimePicker.Location = new System.Drawing.Point(229, 84);
            this.toTimePicker.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.toTimePicker.Name = "toTimePicker";
            this.toTimePicker.Size = new System.Drawing.Size(228, 25);
            this.toTimePicker.TabIndex = 6;
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
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(196, 25);
            this.inputTextBox.TabIndex = 1;
            this.inputTextBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.InputTextbox_MouseClick);
            this.inputTextBox.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.InputTextbox_KeyPress);
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(18, 115);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(88, 18);
            this.label5.TabIndex = 7;
            this.label5.Text = "limit rows";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(15, 85);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(104, 18);
            this.label4.TabIndex = 5;
            this.label4.Text = "timestamp to";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(15, 55);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(120, 18);
            this.label3.TabIndex = 3;
            this.label3.Text = "timestamp from";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(15, 25);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(192, 18);
            this.label2.TabIndex = 0;
            this.label2.Text = "name of table or stable";
            // 
            // import
            // 
            this.import.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.import.Location = new System.Drawing.Point(429, 545);
            this.import.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(70, 29);
            this.import.TabIndex = 4;
            this.import.Text = "Import";
            this.import.UseVisualStyleBackColor = true;
            this.import.Click += new System.EventHandler(this.Import_Click);
            // 
            // TDDetailsForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(509, 586);
            this.Controls.Add(this.rawListView);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.import);
            this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.KeyPreview = true;
            this.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TDDetailsForm";
            this.Text = "Query Detail Data";
            this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.Form_KeyPress);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.limitrowsNumericUpDown)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.CheckBox headsCheck;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button outputButton;
        private System.Windows.Forms.TextBox outputTextbox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.Button inputButton;
        private System.Windows.Forms.TextBox inputTextBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button import;
        private System.Windows.Forms.ListView rawListView;
        private System.Windows.Forms.DateTimePicker toTimePicker;
        private System.Windows.Forms.DateTimePicker fromTimePicker;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.CheckBox ascCheck;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.NumericUpDown limitrowsNumericUpDown;
        private System.Windows.Forms.CheckBox timestampCheck;
    }
}
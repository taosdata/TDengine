namespace excel2010
{
    partial class TDAggregationForm
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
      System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(TDAggregationForm));
      this.groupBox1 = new System.Windows.Forms.GroupBox();
      this.fillValueNumericUpDown = new System.Windows.Forms.NumericUpDown();
      this.intervalTimeNumericUpDown = new System.Windows.Forms.NumericUpDown();
      this.groupbyCheckBox = new System.Windows.Forms.CheckBox();
      this.fillMethodCombox = new System.Windows.Forms.ComboBox();
      this.groupbyComboBox = new System.Windows.Forms.ComboBox();
      this.intervalTimeUnitComboBox = new System.Windows.Forms.ComboBox();
      this.timestampCheck = new System.Windows.Forms.CheckBox();
      this.intervalCheckBox = new System.Windows.Forms.CheckBox();
      this.headsCheck = new System.Windows.Forms.CheckBox();
      this.label5 = new System.Windows.Forms.Label();
      this.label7 = new System.Windows.Forms.Label();
      this.groupBox2 = new System.Windows.Forms.GroupBox();
      this.outputButton = new System.Windows.Forms.Button();
      this.outputTextbox = new System.Windows.Forms.TextBox();
      this.label1 = new System.Windows.Forms.Label();
      this.groupBox3 = new System.Windows.Forms.GroupBox();
      this.toTimePicker = new System.Windows.Forms.DateTimePicker();
      this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
      this.label4 = new System.Windows.Forms.Label();
      this.label3 = new System.Windows.Forms.Label();
      this.inputButton = new System.Windows.Forms.Button();
      this.inputTextBox = new System.Windows.Forms.TextBox();
      this.typeLabel = new System.Windows.Forms.Label();
      this.label2 = new System.Windows.Forms.Label();
      this.selectListView = new System.Windows.Forms.ListView();
      this.rawListView = new System.Windows.Forms.ListView();
      this.unselectButton = new System.Windows.Forms.Button();
      this.import = new System.Windows.Forms.Button();
      this.functionCombox = new System.Windows.Forms.ComboBox();
      this.groupBox1.SuspendLayout();
      ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).BeginInit();
      ((System.ComponentModel.ISupportInitialize)(this.intervalTimeNumericUpDown)).BeginInit();
      this.groupBox2.SuspendLayout();
      this.groupBox3.SuspendLayout();
      this.SuspendLayout();
      // 
      // groupBox1
      // 
      this.groupBox1.Controls.Add(this.fillValueNumericUpDown);
      this.groupBox1.Controls.Add(this.intervalTimeNumericUpDown);
      this.groupBox1.Controls.Add(this.groupbyCheckBox);
      this.groupBox1.Controls.Add(this.fillMethodCombox);
      this.groupBox1.Controls.Add(this.groupbyComboBox);
      this.groupBox1.Controls.Add(this.intervalTimeUnitComboBox);
      this.groupBox1.Controls.Add(this.timestampCheck);
      this.groupBox1.Controls.Add(this.intervalCheckBox);
      this.groupBox1.Controls.Add(this.headsCheck);
      this.groupBox1.Controls.Add(this.label5);
      this.groupBox1.Controls.Add(this.label7);
      this.groupBox1.Location = new System.Drawing.Point(15, 115);
      this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox1.Name = "groupBox1";
      this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox1.Size = new System.Drawing.Size(722, 95);
      this.groupBox1.TabIndex = 0;
      this.groupBox1.TabStop = false;
      this.groupBox1.Text = "Options";
      // 
      // fillValueNumericUpDown
      // 
      this.fillValueNumericUpDown.Location = new System.Drawing.Point(626, 54);
      this.fillValueNumericUpDown.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.fillValueNumericUpDown.Maximum = new decimal(new int[] {
            10000000,
            0,
            0,
            0});
      this.fillValueNumericUpDown.Minimum = new decimal(new int[] {
            10000000,
            0,
            0,
            -2147483648});
      this.fillValueNumericUpDown.Name = "fillValueNumericUpDown";
      this.fillValueNumericUpDown.Size = new System.Drawing.Size(76, 25);
      this.fillValueNumericUpDown.TabIndex = 8;
      // 
      // intervalTimeNumericUpDown
      // 
      this.intervalTimeNumericUpDown.Location = new System.Drawing.Point(120, 54);
      this.intervalTimeNumericUpDown.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.intervalTimeNumericUpDown.Maximum = new decimal(new int[] {
            10000000,
            0,
            0,
            0});
      this.intervalTimeNumericUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
      this.intervalTimeNumericUpDown.Name = "intervalTimeNumericUpDown";
      this.intervalTimeNumericUpDown.Size = new System.Drawing.Size(59, 25);
      this.intervalTimeNumericUpDown.TabIndex = 8;
      this.intervalTimeNumericUpDown.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
      // 
      // groupbyCheckBox
      // 
      this.groupbyCheckBox.AutoSize = true;
      this.groupbyCheckBox.Location = new System.Drawing.Point(400, 25);
      this.groupbyCheckBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupbyCheckBox.Name = "groupbyCheckBox";
      this.groupbyCheckBox.Size = new System.Drawing.Size(91, 22);
      this.groupbyCheckBox.TabIndex = 1;
      this.groupbyCheckBox.Text = "group by";
      this.groupbyCheckBox.UseVisualStyleBackColor = true;
      this.groupbyCheckBox.CheckedChanged += new System.EventHandler(this.GroupbyCheckBox_CheckedChanged);
      // 
      // fillMethodCombox
      // 
      this.fillMethodCombox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
      this.fillMethodCombox.FormattingEnabled = true;
      this.fillMethodCombox.Location = new System.Drawing.Point(467, 54);
      this.fillMethodCombox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.fillMethodCombox.Name = "fillMethodCombox";
      this.fillMethodCombox.Size = new System.Drawing.Size(80, 26);
      this.fillMethodCombox.TabIndex = 9;
      this.fillMethodCombox.SelectedIndexChanged += new System.EventHandler(this.FillMethodCombox_SelectedIndexChanged);
      // 
      // groupbyComboBox
      // 
      this.groupbyComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
      this.groupbyComboBox.FormattingEnabled = true;
      this.groupbyComboBox.Location = new System.Drawing.Point(515, 24);
      this.groupbyComboBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupbyComboBox.Name = "groupbyComboBox";
      this.groupbyComboBox.Size = new System.Drawing.Size(187, 26);
      this.groupbyComboBox.TabIndex = 9;
      // 
      // intervalTimeUnitComboBox
      // 
      this.intervalTimeUnitComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
      this.intervalTimeUnitComboBox.FormattingEnabled = true;
      this.intervalTimeUnitComboBox.Location = new System.Drawing.Point(198, 54);
      this.intervalTimeUnitComboBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.intervalTimeUnitComboBox.Name = "intervalTimeUnitComboBox";
      this.intervalTimeUnitComboBox.Size = new System.Drawing.Size(84, 26);
      this.intervalTimeUnitComboBox.TabIndex = 9;
      // 
      // timestampCheck
      // 
      this.timestampCheck.AutoSize = true;
      this.timestampCheck.Location = new System.Drawing.Point(174, 25);
      this.timestampCheck.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.timestampCheck.Name = "timestampCheck";
      this.timestampCheck.Size = new System.Drawing.Size(187, 22);
      this.timestampCheck.TabIndex = 1;
      this.timestampCheck.Text = "display as timestamp";
      this.timestampCheck.UseVisualStyleBackColor = true;
      // 
      // intervalCheckBox
      // 
      this.intervalCheckBox.AutoSize = true;
      this.intervalCheckBox.Location = new System.Drawing.Point(15, 55);
      this.intervalCheckBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.intervalCheckBox.Name = "intervalCheckBox";
      this.intervalCheckBox.Size = new System.Drawing.Size(91, 22);
      this.intervalCheckBox.TabIndex = 0;
      this.intervalCheckBox.Text = "interval";
      this.intervalCheckBox.UseVisualStyleBackColor = true;
      this.intervalCheckBox.CheckedChanged += new System.EventHandler(this.IntervalCheckBox_CheckedChanged);
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
      // label5
      // 
      this.label5.AutoSize = true;
      this.label5.Location = new System.Drawing.Point(567, 55);
      this.label5.Name = "label5";
      this.label5.Size = new System.Drawing.Size(40, 18);
      this.label5.TabIndex = 7;
      this.label5.Text = "with";
      // 
      // label7
      // 
      this.label7.AutoSize = true;
      this.label7.Location = new System.Drawing.Point(301, 55);
      this.label7.Name = "label7";
      this.label7.Size = new System.Drawing.Size(136, 18);
      this.label7.TabIndex = 7;
      this.label7.Text = "interpolation by";
      // 
      // groupBox2
      // 
      this.groupBox2.Controls.Add(this.outputButton);
      this.groupBox2.Controls.Add(this.outputTextbox);
      this.groupBox2.Controls.Add(this.label1);
      this.groupBox2.Location = new System.Drawing.Point(15, 410);
      this.groupBox2.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox2.Name = "groupBox2";
      this.groupBox2.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox2.Size = new System.Drawing.Size(722, 65);
      this.groupBox2.TabIndex = 5;
      this.groupBox2.TabStop = false;
      this.groupBox2.Text = "Output";
      // 
      // outputButton
      // 
      this.outputButton.Image = ((System.Drawing.Image)(resources.GetObject("outputButton.Image")));
      this.outputButton.Location = new System.Drawing.Point(678, 24);
      this.outputButton.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.outputButton.Name = "outputButton";
      this.outputButton.Size = new System.Drawing.Size(26, 26);
      this.outputButton.TabIndex = 2;
      this.outputButton.UseVisualStyleBackColor = true;
      this.outputButton.Click += new System.EventHandler(this.OutputButton_Click);
      // 
      // outputTextbox
      // 
      this.outputTextbox.Location = new System.Drawing.Point(442, 24);
      this.outputTextbox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.outputTextbox.Name = "outputTextbox";
      this.outputTextbox.Size = new System.Drawing.Size(228, 25);
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
      this.groupBox3.Controls.Add(this.toTimePicker);
      this.groupBox3.Controls.Add(this.fromTimePicker);
      this.groupBox3.Controls.Add(this.label4);
      this.groupBox3.Controls.Add(this.label3);
      this.groupBox3.Controls.Add(this.inputButton);
      this.groupBox3.Controls.Add(this.inputTextBox);
      this.groupBox3.Controls.Add(this.typeLabel);
      this.groupBox3.Controls.Add(this.label2);
      this.groupBox3.Location = new System.Drawing.Point(15, 10);
      this.groupBox3.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox3.Name = "groupBox3";
      this.groupBox3.Padding = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.groupBox3.Size = new System.Drawing.Size(722, 95);
      this.groupBox3.TabIndex = 0;
      this.groupBox3.TabStop = false;
      this.groupBox3.Text = "Input";
      // 
      // toTimePicker
      // 
      this.toTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
      this.toTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
      this.toTimePicker.Location = new System.Drawing.Point(468, 54);
      this.toTimePicker.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.toTimePicker.Name = "toTimePicker";
      this.toTimePicker.Size = new System.Drawing.Size(234, 25);
      this.toTimePicker.TabIndex = 6;
      // 
      // fromTimePicker
      // 
      this.fromTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
      this.fromTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
      this.fromTimePicker.Location = new System.Drawing.Point(168, 54);
      this.fromTimePicker.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.fromTimePicker.Name = "fromTimePicker";
      this.fromTimePicker.Size = new System.Drawing.Size(234, 25);
      this.fromTimePicker.TabIndex = 4;
      // 
      // label4
      // 
      this.label4.AutoSize = true;
      this.label4.Location = new System.Drawing.Point(424, 55);
      this.label4.Name = "label4";
      this.label4.Size = new System.Drawing.Size(24, 18);
      this.label4.TabIndex = 5;
      this.label4.Text = "to";
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
      // inputButton
      // 
      this.inputButton.Image = ((System.Drawing.Image)(resources.GetObject("inputButton.Image")));
      this.inputButton.Location = new System.Drawing.Point(678, 24);
      this.inputButton.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.inputButton.Name = "inputButton";
      this.inputButton.Size = new System.Drawing.Size(26, 26);
      this.inputButton.TabIndex = 2;
      this.inputButton.UseVisualStyleBackColor = true;
      this.inputButton.Click += new System.EventHandler(this.InputButton_Click);
      // 
      // inputTextBox
      // 
      this.inputTextBox.Location = new System.Drawing.Point(306, 24);
      this.inputTextBox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.inputTextBox.MaxLength = 64000;
      this.inputTextBox.Name = "inputTextBox";
      this.inputTextBox.Size = new System.Drawing.Size(366, 25);
      this.inputTextBox.TabIndex = 1;
      this.inputTextBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.InputTextbox_MouseClick);
      this.inputTextBox.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.InputTextbox_KeyPress);
      // 
      // typeLabel
      // 
      this.typeLabel.AutoSize = true;
      this.typeLabel.Location = new System.Drawing.Point(368, 52);
      this.typeLabel.Name = "typeLabel";
      this.typeLabel.Size = new System.Drawing.Size(0, 18);
      this.typeLabel.TabIndex = 0;
      // 
      // label2
      // 
      this.label2.AutoSize = true;
      this.label2.Location = new System.Drawing.Point(15, 25);
      this.label2.Name = "label2";
      this.label2.Size = new System.Drawing.Size(248, 18);
      this.label2.TabIndex = 0;
      this.label2.Text = "name of tables or stable (N*N)";
      // 
      // selectListView
      // 
      this.selectListView.Location = new System.Drawing.Point(497, 220);
      this.selectListView.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.selectListView.Name = "selectListView";
      this.selectListView.Size = new System.Drawing.Size(240, 180);
      this.selectListView.TabIndex = 4;
      this.selectListView.UseCompatibleStateImageBehavior = false;
      this.selectListView.View = System.Windows.Forms.View.Details;
      // 
      // rawListView
      // 
      this.rawListView.Location = new System.Drawing.Point(15, 220);
      this.rawListView.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.rawListView.Name = "rawListView";
      this.rawListView.Size = new System.Drawing.Size(392, 180);
      this.rawListView.TabIndex = 1;
      this.rawListView.UseCompatibleStateImageBehavior = false;
      this.rawListView.View = System.Windows.Forms.View.Details;
      // 
      // unselectButton
      // 
      this.unselectButton.Location = new System.Drawing.Point(418, 275);
      this.unselectButton.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.unselectButton.Name = "unselectButton";
      this.unselectButton.Size = new System.Drawing.Size(70, 29);
      this.unselectButton.TabIndex = 3;
      this.unselectButton.Text = "<<";
      this.unselectButton.UseVisualStyleBackColor = true;
      this.unselectButton.Click += new System.EventHandler(this.UnselectButton_Click);
      // 
      // import
      // 
      this.import.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
      this.import.Location = new System.Drawing.Point(671, 485);
      this.import.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.import.Name = "import";
      this.import.Size = new System.Drawing.Size(69, 29);
      this.import.TabIndex = 6;
      this.import.Text = "Import";
      this.import.UseVisualStyleBackColor = true;
      this.import.Click += new System.EventHandler(this.Import_Click);
      // 
      // functionCombox
      // 
      this.functionCombox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
      this.functionCombox.FormattingEnabled = true;
      this.functionCombox.Location = new System.Drawing.Point(418, 240);
      this.functionCombox.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.functionCombox.Name = "functionCombox";
      this.functionCombox.Size = new System.Drawing.Size(70, 26);
      this.functionCombox.TabIndex = 2;
      this.functionCombox.SelectedIndexChanged += new System.EventHandler(this.SelectCombox_SelectedIndexChanged);
      // 
      // TDAggregationForm
      // 
      this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 18F);
      this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
      this.ClientSize = new System.Drawing.Size(750, 527);
      this.Controls.Add(this.functionCombox);
      this.Controls.Add(this.groupBox1);
      this.Controls.Add(this.groupBox2);
      this.Controls.Add(this.groupBox3);
      this.Controls.Add(this.selectListView);
      this.Controls.Add(this.rawListView);
      this.Controls.Add(this.unselectButton);
      this.Controls.Add(this.import);
      this.Font = new System.Drawing.Font("Consolas", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
      this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
      this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
      this.KeyPreview = true;
      this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
      this.MaximizeBox = false;
      this.MinimizeBox = false;
      this.Name = "TDAggregationForm";
      this.Text = "Aggregation query";
      this.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.Form_KeyPress);
      this.groupBox1.ResumeLayout(false);
      this.groupBox1.PerformLayout();
      ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).EndInit();
      ((System.ComponentModel.ISupportInitialize)(this.intervalTimeNumericUpDown)).EndInit();
      this.groupBox2.ResumeLayout(false);
      this.groupBox2.PerformLayout();
      this.groupBox3.ResumeLayout(false);
      this.groupBox3.PerformLayout();
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
        private System.Windows.Forms.ListView selectListView;
        private System.Windows.Forms.ListView rawListView;
        private System.Windows.Forms.Button unselectButton;
        private System.Windows.Forms.Button import;
        private System.Windows.Forms.DateTimePicker toTimePicker;
        private System.Windows.Forms.DateTimePicker fromTimePicker;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.ComboBox intervalTimeUnitComboBox;
        private System.Windows.Forms.NumericUpDown intervalTimeNumericUpDown;
        private System.Windows.Forms.ComboBox functionCombox;
        private System.Windows.Forms.CheckBox timestampCheck;
        private System.Windows.Forms.CheckBox groupbyCheckBox;
        private System.Windows.Forms.ComboBox fillMethodCombox;
        private System.Windows.Forms.ComboBox groupbyComboBox;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.NumericUpDown fillValueNumericUpDown;
        private System.Windows.Forms.CheckBox intervalCheckBox;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label typeLabel;
    }
}
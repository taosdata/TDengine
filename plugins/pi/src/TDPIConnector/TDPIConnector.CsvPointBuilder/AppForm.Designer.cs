
namespace TDPIConnector.CsvPointBuilder
{
    partial class AppForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(AppForm));
            this.lbPointsCsv = new System.Windows.Forms.ListBox();
            this.lbPointsFound = new System.Windows.Forms.ListBox();
            this.grCsvPoints = new System.Windows.Forms.GroupBox();
            this.grPointFound = new System.Windows.Forms.GroupBox();
            this.btnAddToCsv = new System.Windows.Forms.Button();
            this.btnDeletePoints = new System.Windows.Forms.Button();
            this.grSearch = new System.Windows.Forms.GroupBox();
            this.lbInstumentTag = new System.Windows.Forms.Label();
            this.lbDescriptor = new System.Windows.Forms.Label();
            this.tbInstrumentTag = new System.Windows.Forms.TextBox();
            this.tbDescriptor = new System.Windows.Forms.TextBox();
            this.lbPointSource = new System.Windows.Forms.Label();
            this.tbPointSource = new System.Windows.Forms.TextBox();
            this.lbPointName = new System.Windows.Forms.Label();
            this.tbPointName = new System.Windows.Forms.TextBox();
            this.btnSearch = new System.Windows.Forms.Button();
            this.lbSelectPIDataArchive = new System.Windows.Forms.Label();
            this.cbPIDataArchiveNames = new System.Windows.Forms.ComboBox();
            this.btnSaveCsv = new System.Windows.Forms.Button();
            this.grPIDataArchiveConnect = new System.Windows.Forms.GroupBox();
            this.btnConnect = new System.Windows.Forms.Button();
            this.grCsvPoints.SuspendLayout();
            this.grPointFound.SuspendLayout();
            this.grSearch.SuspendLayout();
            this.grPIDataArchiveConnect.SuspendLayout();
            this.SuspendLayout();
            // 
            // lbPointsCsv
            // 
            this.lbPointsCsv.FormattingEnabled = true;
            this.lbPointsCsv.Location = new System.Drawing.Point(27, 19);
            this.lbPointsCsv.Name = "lbPointsCsv";
            this.lbPointsCsv.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lbPointsCsv.Size = new System.Drawing.Size(345, 420);
            this.lbPointsCsv.TabIndex = 0;
            // 
            // lbPointsFound
            // 
            this.lbPointsFound.FormattingEnabled = true;
            this.lbPointsFound.Location = new System.Drawing.Point(17, 19);
            this.lbPointsFound.Name = "lbPointsFound";
            this.lbPointsFound.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lbPointsFound.Size = new System.Drawing.Size(356, 407);
            this.lbPointsFound.TabIndex = 1;
            // 
            // grCsvPoints
            // 
            this.grCsvPoints.Controls.Add(this.lbPointsCsv);
            this.grCsvPoints.Location = new System.Drawing.Point(976, 31);
            this.grCsvPoints.Name = "grCsvPoints";
            this.grCsvPoints.Size = new System.Drawing.Size(384, 452);
            this.grCsvPoints.TabIndex = 2;
            this.grCsvPoints.TabStop = false;
            this.grCsvPoints.Text = "Selected PI Points for CSV";
            // 
            // grPointFound
            // 
            this.grPointFound.Controls.Add(this.lbPointsFound);
            this.grPointFound.Location = new System.Drawing.Point(460, 31);
            this.grPointFound.Name = "grPointFound";
            this.grPointFound.Size = new System.Drawing.Size(390, 439);
            this.grPointFound.TabIndex = 3;
            this.grPointFound.TabStop = false;
            this.grPointFound.Text = "PI Points found:";
            // 
            // btnAddToCsv
            // 
            this.btnAddToCsv.Location = new System.Drawing.Point(878, 224);
            this.btnAddToCsv.Name = "btnAddToCsv";
            this.btnAddToCsv.Size = new System.Drawing.Size(75, 23);
            this.btnAddToCsv.TabIndex = 4;
            this.btnAddToCsv.Text = "==>";
            this.btnAddToCsv.UseVisualStyleBackColor = true;
            this.btnAddToCsv.Click += new System.EventHandler(this.btnAddToCsv_Click);
            // 
            // btnDeletePoints
            // 
            this.btnDeletePoints.Location = new System.Drawing.Point(1087, 498);
            this.btnDeletePoints.Name = "btnDeletePoints";
            this.btnDeletePoints.Size = new System.Drawing.Size(92, 23);
            this.btnDeletePoints.TabIndex = 5;
            this.btnDeletePoints.Text = "Delete PI Points";
            this.btnDeletePoints.UseVisualStyleBackColor = true;
            this.btnDeletePoints.Click += new System.EventHandler(this.btnDeletePoints_Click);
            // 
            // grSearch
            // 
            this.grSearch.Controls.Add(this.lbInstumentTag);
            this.grSearch.Controls.Add(this.lbDescriptor);
            this.grSearch.Controls.Add(this.tbInstrumentTag);
            this.grSearch.Controls.Add(this.tbDescriptor);
            this.grSearch.Controls.Add(this.lbPointSource);
            this.grSearch.Controls.Add(this.tbPointSource);
            this.grSearch.Controls.Add(this.lbPointName);
            this.grSearch.Controls.Add(this.tbPointName);
            this.grSearch.Controls.Add(this.btnSearch);
            this.grSearch.Location = new System.Drawing.Point(42, 138);
            this.grSearch.Name = "grSearch";
            this.grSearch.Size = new System.Drawing.Size(388, 319);
            this.grSearch.TabIndex = 6;
            this.grSearch.TabStop = false;
            this.grSearch.Text = "Search";
            // 
            // lbInstumentTag
            // 
            this.lbInstumentTag.AutoSize = true;
            this.lbInstumentTag.Location = new System.Drawing.Point(15, 194);
            this.lbInstumentTag.Name = "lbInstumentTag";
            this.lbInstumentTag.Size = new System.Drawing.Size(70, 13);
            this.lbInstumentTag.TabIndex = 15;
            this.lbInstumentTag.Text = "IntrumentTag";
            // 
            // lbDescriptor
            // 
            this.lbDescriptor.AutoSize = true;
            this.lbDescriptor.Location = new System.Drawing.Point(15, 139);
            this.lbDescriptor.Name = "lbDescriptor";
            this.lbDescriptor.Size = new System.Drawing.Size(55, 13);
            this.lbDescriptor.TabIndex = 14;
            this.lbDescriptor.Text = "Descriptor";
            // 
            // tbInstrumentTag
            // 
            this.tbInstrumentTag.Location = new System.Drawing.Point(18, 210);
            this.tbInstrumentTag.Name = "tbInstrumentTag";
            this.tbInstrumentTag.Size = new System.Drawing.Size(334, 20);
            this.tbInstrumentTag.TabIndex = 13;
            this.tbInstrumentTag.Text = "*";
            // 
            // tbDescriptor
            // 
            this.tbDescriptor.Location = new System.Drawing.Point(18, 155);
            this.tbDescriptor.Name = "tbDescriptor";
            this.tbDescriptor.Size = new System.Drawing.Size(334, 20);
            this.tbDescriptor.TabIndex = 12;
            this.tbDescriptor.Text = "*";
            // 
            // lbPointSource
            // 
            this.lbPointSource.AutoSize = true;
            this.lbPointSource.Location = new System.Drawing.Point(17, 83);
            this.lbPointSource.Name = "lbPointSource";
            this.lbPointSource.Size = new System.Drawing.Size(71, 13);
            this.lbPointSource.TabIndex = 11;
            this.lbPointSource.Text = "Point Source:";
            // 
            // tbPointSource
            // 
            this.tbPointSource.Location = new System.Drawing.Point(18, 99);
            this.tbPointSource.Name = "tbPointSource";
            this.tbPointSource.Size = new System.Drawing.Size(334, 20);
            this.tbPointSource.TabIndex = 10;
            this.tbPointSource.Text = "*";
            // 
            // lbPointName
            // 
            this.lbPointName.AutoSize = true;
            this.lbPointName.Location = new System.Drawing.Point(18, 35);
            this.lbPointName.Name = "lbPointName";
            this.lbPointName.Size = new System.Drawing.Size(58, 13);
            this.lbPointName.TabIndex = 9;
            this.lbPointName.Text = "Tag Mask:";
            // 
            // tbPointName
            // 
            this.tbPointName.Location = new System.Drawing.Point(18, 51);
            this.tbPointName.Name = "tbPointName";
            this.tbPointName.Size = new System.Drawing.Size(334, 20);
            this.tbPointName.TabIndex = 8;
            this.tbPointName.Text = "*";
            // 
            // btnSearch
            // 
            this.btnSearch.Location = new System.Drawing.Point(259, 264);
            this.btnSearch.Name = "btnSearch";
            this.btnSearch.Size = new System.Drawing.Size(93, 23);
            this.btnSearch.TabIndex = 7;
            this.btnSearch.Text = "Search";
            this.btnSearch.UseVisualStyleBackColor = true;
            this.btnSearch.Click += new System.EventHandler(this.btnSearch_Click);
            // 
            // lbSelectPIDataArchive
            // 
            this.lbSelectPIDataArchive.AutoSize = true;
            this.lbSelectPIDataArchive.Location = new System.Drawing.Point(15, 31);
            this.lbSelectPIDataArchive.Name = "lbSelectPIDataArchive";
            this.lbSelectPIDataArchive.Size = new System.Drawing.Size(118, 13);
            this.lbSelectPIDataArchive.TabIndex = 1;
            this.lbSelectPIDataArchive.Text = "Select PI Data Archive:";
            // 
            // cbPIDataArchiveNames
            // 
            this.cbPIDataArchiveNames.FormattingEnabled = true;
            this.cbPIDataArchiveNames.Location = new System.Drawing.Point(18, 47);
            this.cbPIDataArchiveNames.Name = "cbPIDataArchiveNames";
            this.cbPIDataArchiveNames.Size = new System.Drawing.Size(258, 21);
            this.cbPIDataArchiveNames.TabIndex = 0;
            // 
            // btnSaveCsv
            // 
            this.btnSaveCsv.Location = new System.Drawing.Point(1242, 498);
            this.btnSaveCsv.Name = "btnSaveCsv";
            this.btnSaveCsv.Size = new System.Drawing.Size(92, 23);
            this.btnSaveCsv.TabIndex = 7;
            this.btnSaveCsv.Text = "Save CSV";
            this.btnSaveCsv.UseVisualStyleBackColor = true;
            this.btnSaveCsv.Click += new System.EventHandler(this.btnSaveCsv_Click);
            // 
            // grPIDataArchiveConnect
            // 
            this.grPIDataArchiveConnect.Controls.Add(this.btnConnect);
            this.grPIDataArchiveConnect.Controls.Add(this.cbPIDataArchiveNames);
            this.grPIDataArchiveConnect.Controls.Add(this.lbSelectPIDataArchive);
            this.grPIDataArchiveConnect.Location = new System.Drawing.Point(42, 31);
            this.grPIDataArchiveConnect.Name = "grPIDataArchiveConnect";
            this.grPIDataArchiveConnect.Size = new System.Drawing.Size(388, 89);
            this.grPIDataArchiveConnect.TabIndex = 8;
            this.grPIDataArchiveConnect.TabStop = false;
            this.grPIDataArchiveConnect.Text = "Connect to PI Data Archive";
            // 
            // btnConnect
            // 
            this.btnConnect.Location = new System.Drawing.Point(282, 47);
            this.btnConnect.Name = "btnConnect";
            this.btnConnect.Size = new System.Drawing.Size(70, 23);
            this.btnConnect.TabIndex = 16;
            this.btnConnect.Text = "Connect";
            this.btnConnect.UseVisualStyleBackColor = true;
            this.btnConnect.Click += new System.EventHandler(this.btnConnect_Click);
            // 
            // AppForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1381, 553);
            this.Controls.Add(this.grPIDataArchiveConnect);
            this.Controls.Add(this.btnSaveCsv);
            this.Controls.Add(this.grSearch);
            this.Controls.Add(this.btnDeletePoints);
            this.Controls.Add(this.btnAddToCsv);
            this.Controls.Add(this.grPointFound);
            this.Controls.Add(this.grCsvPoints);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "AppForm";
            this.Text = "CSV Point Builder";
            this.grCsvPoints.ResumeLayout(false);
            this.grPointFound.ResumeLayout(false);
            this.grSearch.ResumeLayout(false);
            this.grSearch.PerformLayout();
            this.grPIDataArchiveConnect.ResumeLayout(false);
            this.grPIDataArchiveConnect.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.ListBox lbPointsCsv;
        private System.Windows.Forms.ListBox lbPointsFound;
        private System.Windows.Forms.GroupBox grCsvPoints;
        private System.Windows.Forms.GroupBox grPointFound;
        private System.Windows.Forms.Button btnAddToCsv;
        private System.Windows.Forms.Button btnDeletePoints;
        private System.Windows.Forms.GroupBox grSearch;
        private System.Windows.Forms.Label lbInstumentTag;
        private System.Windows.Forms.Label lbDescriptor;
        private System.Windows.Forms.TextBox tbInstrumentTag;
        private System.Windows.Forms.TextBox tbDescriptor;
        private System.Windows.Forms.Label lbPointSource;
        private System.Windows.Forms.TextBox tbPointSource;
        private System.Windows.Forms.Label lbPointName;
        private System.Windows.Forms.TextBox tbPointName;
        private System.Windows.Forms.Button btnSearch;
        private System.Windows.Forms.Label lbSelectPIDataArchive;
        private System.Windows.Forms.ComboBox cbPIDataArchiveNames;
        private System.Windows.Forms.Button btnSaveCsv;
        private System.Windows.Forms.GroupBox grPIDataArchiveConnect;
        private System.Windows.Forms.Button btnConnect;
    }
}


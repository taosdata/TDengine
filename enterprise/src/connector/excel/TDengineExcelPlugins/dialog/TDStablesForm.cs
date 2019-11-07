using System;
using System.Linq;
using System.Windows.Forms;
using System.Runtime.InteropServices;
using Newtonsoft.Json.Linq;
using Excel = Microsoft.Office.Interop.Excel;
using Range = Microsoft.Office.Interop.Excel.Range;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDStablesForm : TDControl
    {
        private TableLayoutPanel tableLayoutPanel1;
        private CheckBox headsCheck;
        private CheckBox basicinfoCheck;
        private Label label1;
        private TableLayoutPanel tableLayoutPanel2;
        private Button outputButton;
        private Label label2;
        private Label label3;
        private Label label4;
        private TableLayoutPanel tableLayoutPanel3;
        private Button import;
        private TextBox outputTextbox;
        public Label TheLabel;
        public TDStablesForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();
        }

        public override void Initialize()
        {
            this.outputTextbox.Text = TDFactory.Persist.stablesOutput;
            this.headsCheck.Checked = TDFactory.Persist.stablesShowHeads;
            this.basicinfoCheck.Checked = TDFactory.Persist.stablesShowBasicInfo;
        }

        public override void Save()
        {
            TDFactory.Persist.stablesOutput = this.outputTextbox.Text;
            TDFactory.Persist.stablesShowHeads = this.headsCheck.Checked;
            TDFactory.Persist.stablesShowBasicInfo = this.basicinfoCheck.Checked;
        }
        
        private void OutputButton_Click(object sender, EventArgs e)
        {
            String address = TDFactory.Excel.GetSelectionAddress();
            this.outputTextbox.Text = address.Replace("$", String.Empty); ;
        }

        private void Import_Click(object sender, EventArgs e)
        {
            TDFactory.Persist.Save();

            String sql = this.GenerateSql();

            Range outputRange = TDFactory.Excel.GetFirstRangeByRangeAddress(outputTextbox.Text);
            if (outputRange == null)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_OUTPUT_NOT_SELECT);
            }
            else
            {
                TDHttpReturn resp = TDFactory.Util.DoRequest(sql, TDHttpTimestampType.TD_SHOW_TIME_STRING).Result;
                if (resp.jo != null)
                {
                    TDFactory.StartUpdate();
                    this.FillExcel(resp.jo, outputRange);
                    TDFactory.EndUpdate();
                }
                else
                {
                    TDFactory.Util.ShowError(resp.error);
                }
            }
        }

        private void FillExcel(JObject jo, Range range)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;
                bool showBasicInfo = this.basicinfoCheck.Checked;

                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                range.UnMerge();
                int beginRow = range.Row;
                int beginCol = range.Column;

                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                if (!showBasicInfo)
                {
                    headLength = Math.Min(1, headLength);
                }

                int beginOffset = 0;
                int[] rowArray = TDFactory.Excel.GetUnHiddenRows(beginRow, showHeads ? dataLength + 1 : dataLength);
                int[] colArray = TDFactory.Excel.GetUnHiddenColumns(beginCol, headLength);

                if (showHeads)
                {
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[rowArray[0], colArray[col]].Value2 = heads.GetValue(col).ToString();
                    }
                    beginOffset++;
                }

                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[rowArray[row + beginOffset], colArray[col]].Value2 = dataCols.GetValue(col).ToString();
                    }
                }

                if (dataLength == 0)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_DATA_FROM_SERVER_EMPTY);
                    return;
                }
            }
            catch (Exception e)
            {
                TDFactory.Util.ShowException(e);
            }
            finally { }
        }

        private String GenerateSql()
        {
            return "show " + TDFactory.Persist.connectDB + ".stables";
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.basicinfoCheck = new System.Windows.Forms.CheckBox();
            this.label1 = new System.Windows.Forms.Label();
            this.tableLayoutPanel2 = new System.Windows.Forms.TableLayoutPanel();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.tableLayoutPanel3 = new System.Windows.Forms.TableLayoutPanel();
            this.import = new System.Windows.Forms.Button();
            this.tableLayoutPanel1.SuspendLayout();
            this.tableLayoutPanel2.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.headsCheck, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.basicinfoCheck, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel2, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.label3, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 8);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 10;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 400);
            this.tableLayoutPanel1.TabIndex = 0;
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.headsCheck.Location = new System.Drawing.Point(18, 46);
            this.headsCheck.Name = "headsCheck";
            this.headsCheck.Size = new System.Drawing.Size(464, 26);
            this.headsCheck.TabIndex = 1;
            this.headsCheck.Text = "show heads";
            this.headsCheck.UseVisualStyleBackColor = true;
            // 
            // basicinfoCheck
            // 
            this.basicinfoCheck.AutoSize = true;
            this.basicinfoCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.basicinfoCheck.Location = new System.Drawing.Point(18, 78);
            this.basicinfoCheck.Name = "basicinfoCheck";
            this.basicinfoCheck.Size = new System.Drawing.Size(464, 26);
            this.basicinfoCheck.TabIndex = 2;
            this.basicinfoCheck.Text = "show basic information";
            this.basicinfoCheck.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 138);
            this.label1.Margin = new System.Windows.Forms.Padding(3);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(464, 22);
            this.label1.TabIndex = 3;
            this.label1.Text = "select start cell (1x1)";
            // 
            // tableLayoutPanel2
            // 
            this.tableLayoutPanel2.ColumnCount = 2;
            this.tableLayoutPanel2.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel2.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel2.Controls.Add(this.outputTextbox, 0, 0);
            this.tableLayoutPanel2.Controls.Add(this.outputButton, 1, 0);
            this.tableLayoutPanel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel2.Location = new System.Drawing.Point(18, 166);
            this.tableLayoutPanel2.Name = "tableLayoutPanel2";
            this.tableLayoutPanel2.RowCount = 1;
            this.tableLayoutPanel2.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel2.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel2.TabIndex = 5;
            // 
            // outputTextbox
            // 
            this.outputTextbox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.outputTextbox.Location = new System.Drawing.Point(0, 3);
            this.outputTextbox.Margin = new System.Windows.Forms.Padding(0, 3, 3, 3);
            this.outputTextbox.Name = "outputTextbox";
            this.outputTextbox.Size = new System.Drawing.Size(345, 29);
            this.outputTextbox.TabIndex = 5;
            // 
            // outputButton
            // 
            this.outputButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.outputButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.outputButton.Location = new System.Drawing.Point(351, 3);
            this.outputButton.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.outputButton.Name = "outputButton";
            this.outputButton.Size = new System.Drawing.Size(113, 29);
            this.outputButton.TabIndex = 0;
            this.outputButton.Text = "Select";
            this.outputButton.UseVisualStyleBackColor = true;
            this.outputButton.Click += new System.EventHandler(this.OutputButton_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Location = new System.Drawing.Point(18, 110);
            this.label2.Margin = new System.Windows.Forms.Padding(3);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(464, 22);
            this.label2.TabIndex = 6;
            this.label2.Text = "Output";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label3.Location = new System.Drawing.Point(18, 18);
            this.label3.Margin = new System.Windows.Forms.Padding(3);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(464, 22);
            this.label3.TabIndex = 7;
            this.label3.Text = "Input";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label4.Location = new System.Drawing.Point(18, 207);
            this.label4.Margin = new System.Windows.Forms.Padding(3);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(464, 22);
            this.label4.TabIndex = 8;
            this.label4.Text = "Operation";
            // 
            // tableLayoutPanel3
            // 
            this.tableLayoutPanel3.ColumnCount = 2;
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.Controls.Add(this.import, 1, 0);
            this.tableLayoutPanel3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 235);
            this.tableLayoutPanel3.Name = "tableLayoutPanel3";
            this.tableLayoutPanel3.RowCount = 1;
            this.tableLayoutPanel3.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel3.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel3.TabIndex = 9;
            // 
            // import
            // 
            this.import.Dock = System.Windows.Forms.DockStyle.Fill;
            this.import.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.import.Location = new System.Drawing.Point(351, 3);
            this.import.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(113, 29);
            this.import.TabIndex = 0;
            this.import.Text = "Import";
            this.import.UseVisualStyleBackColor = true;
            this.import.Click += new System.EventHandler(this.Import_Click);
            // 
            // TDStablesForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 400);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDStablesForm";
            this.Size = new System.Drawing.Size(500, 400);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.tableLayoutPanel2.ResumeLayout(false);
            this.tableLayoutPanel2.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.ResumeLayout(false);

        }
    }
}

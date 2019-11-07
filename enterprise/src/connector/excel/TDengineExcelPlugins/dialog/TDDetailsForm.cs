using System.Runtime.InteropServices;
using System.Windows.Forms;
using System.Linq;
using Excel = Microsoft.Office.Interop.Excel;
using Range = Microsoft.Office.Interop.Excel.Range;
using Newtonsoft.Json.Linq;
using System;
using System.Text;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDDetailsForm : TDControl
    {
        private TextBox inputTextBox;
        private CheckBox headsCheck;
        private CheckBox timestampCheck;
        private TableLayoutPanel tableLayoutPanel2;
        private TextBox outputTextbox;
        private Button outputButton;
        private Label label3;
        private Label label4;
        private TableLayoutPanel tableLayoutPanel3;
        private Button import;
        private Label label5;
        private Label label6;
        private TableLayoutPanel tableLayoutPanel4;
        private Button inputButton;
        private TableLayoutPanel tableLayoutPanel1;
        private NumericUpDown limitrowsNumericUpDown;
        private Label label8;
        private Label label9;
        private Label label10;
        private DateTimePicker toTimePicker;
        private DateTimePicker fromTimePicker;
        private TableLayoutPanel tableLayoutPanel5;
        private Label label11;
        private Button showButton;
        private ListView rawListView;
        private Label label1;
        private Label label2;
        private CheckBox ascCheck;
        public Label TheLabel;
        public TDDetailsForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();
            this.limitrowsNumericUpDown.Value = 1000;
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = TDFactory.Persist.detailInput;
            this.FillListViewInitial();

            if (TDFactory.Persist.detailFromTimestamp == 0 && TDFactory.Persist.detailToTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                DateTime end = (DateTime.Now.AddDays(1)).Date;
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }
            else
            {
                DateTime begin = new DateTime(TDFactory.Persist.detailFromTimestamp);
                DateTime end = new DateTime(TDFactory.Persist.detailToTimestamp);
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }

            if (TDFactory.Persist.detailLimitRows >= 1 && TDFactory.Persist.detailLimitRows <= 100000)
                this.limitrowsNumericUpDown.Value = TDFactory.Persist.detailLimitRows;

            this.headsCheck.Checked = TDFactory.Persist.detailShowHeads;
            this.timestampCheck.Checked = TDFactory.Persist.detailDisplayAsTimestamp;
            this.ascCheck.Checked = TDFactory.Persist.detailAscend;
            
            this.outputTextbox.Text = TDFactory.Persist.detailOutput;
        }

        public override void Save()
        {
            TDFactory.Persist.detailInput = this.inputTextBox.Text;
            TDFactory.Persist.detailFromTimestamp = fromTimePicker.Value.Ticks;
            TDFactory.Persist.detailToTimestamp = toTimePicker.Value.Ticks;
            TDFactory.Persist.detailLimitRows = Decimal.ToInt32(this.limitrowsNumericUpDown.Value);

            TDFactory.Persist.detailShowHeads = this.headsCheck.Checked;
            TDFactory.Persist.detailDisplayAsTimestamp = this.timestampCheck.Checked;
            TDFactory.Persist.detailAscend = this.ascCheck.Checked;
            
            TDFactory.Persist.detailSelectFields.Clear();
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                TDFactory.Persist.detailSelectFields.Add(lvi.Text);
            }

            TDFactory.Persist.detailOutput = this.outputTextbox.Text;
        }
        
        private void InputButton_Click(object sender, EventArgs e)
        {
            this.inputTextBox.Text = TDFactory.Excel.GetSelectionValue();
            if (this.inputTextBox.Text != TDFactory.Util.TD_TABLE_EMPTY_SELECTION) this.FillListView();
        }
        
        private void ShowButton_Click(object sender, EventArgs e)
        {
            if (this.inputTextBox.Text != TDFactory.Util.TD_TABLE_EMPTY_SELECTION) this.FillListView();
        }
        
        private void OutputButton_Click(object sender, EventArgs e)
        {
            String address = TDFactory.Excel.GetSelectionAddress();
            this.outputTextbox.Text = address.Replace("$", String.Empty);
        }

        private void Import_Click(object sender, EventArgs e)
        {
            TDFactory.Persist.Save();

            Range outputRange = TDFactory.Excel.GetFirstRangeByRangeAddress(outputTextbox.Text);
            if (outputRange == null)
            {
                TDFactory.Util.ShowError("output columns not select");
                return;
            }

            TDSingleTable table = TDFactory.Util.ExplainSingleTable(this.inputTextBox.Text).Result;
            if (table.error != String.Empty)
            {
                TDFactory.Util.ShowError(table.error);
                return;
            }

            String[] selectedFields = this.GetListViewCheckedItems();
            String from = fromTimePicker.Text;
            String to = toTimePicker.Text;
            bool asc = ascCheck.Checked;
            int limitRows = Decimal.ToInt32(this.limitrowsNumericUpDown.Value);
            if (asc)
            {
                if (table.isStable)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_ASCENDING_ONLY_SUPPORT_TABLE);
                    asc = false;
                    ascCheck.Checked = false;
                }
            }

            String sql = this.GenerateSql(TDFactory.Persist.connectDB, table.table, selectedFields, from, to, asc, limitRows);

            TDHttpTimestampType displayAsTimestamp = this.timestampCheck.Checked ? TDHttpTimestampType.TD_SHOW_TIMESTSAMP : TDHttpTimestampType.TD_SHOW_TIME_STRING;
            TDHttpReturn resp = TDFactory.Util.DoRequest(sql, displayAsTimestamp).Result;
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

        private void FillListView()
        {
            this.ClearListView();

            TDSingleTable table = TDFactory.Util.ExplainSingleTable(this.inputTextBox.Text).Result;
            if (table.error != String.Empty)
            {
                TDFactory.Util.ShowError(table.error);
                return;
            }

            String sql = "describe " + TDFactory.Persist.connectDB + "." + table.table;
            TDHttpReturn resp = TDFactory.Util.DoRequest(sql, TDHttpTimestampType.TD_SHOW_TIMESTSAMP).Result;
            JObject jo = resp.jo;
            if (jo != null)
            {
                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                if (headLength != 4)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_RESPONSE_FROM_SERVER);
                    return;
                }

                if (dataLength < 2)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_TABLE_DESCRIBE_FAILED);
                    return;
                }

                this.rawListView.BeginUpdate();

                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String field = dataCols.GetValue(0).ToString();
                    String fieldType = dataCols.GetValue(1).ToString();
                    String tag = dataCols.GetValue(3).ToString();

                    //jump the tag fields of tables
                    if (tag != "" && !table.isStable)
                    {
                        continue;
                    }

                    if (fieldType == "BINARY" || fieldType == "NCHAR")
                    {
                        fieldType = fieldType + "(" + dataCols.GetValue(2).ToString() + ")";
                    }

                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = field;
                    lvi.SubItems.Add(fieldType.ToLower());
                    this.rawListView.Items.Add(lvi);
                }

                if (table.isStable)
                {
                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = "tbname";
                    lvi.SubItems.Add("binary(32)");
                    this.rawListView.Items.Add(lvi);
                }

                this.rawListView.EndUpdate();
            }
            else
            {
                TDFactory.Util.ShowError(resp.error);
            }
        }

        private void ClearListView()
        {
            this.rawListView.BeginUpdate();
            this.rawListView.Clear();

            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = TDFactory.Util.TD_FIELD_NAME_COLUMN;
            ch1.Width = 280;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);

            ColumnHeader ch2 = new ColumnHeader();
            ch2.Text = TDFactory.Util.TD_FIELD_TYPE_COLUMN;
            ch2.Width = 120;
            ch2.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch2);

            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.rawListView.CheckBoxes = true;

            this.rawListView.EndUpdate();
        }

        private void FillListViewInitial()
        {
            this.ClearListView();
            foreach (String field in TDFactory.Persist.detailSelectFields)
            {
                ListViewItem lvi = new ListViewItem();
                lvi.Text = field;
                lvi.Checked = true;
                this.rawListView.Items.Add(lvi);
            }
        }

        private String[] GetListViewCheckedItems()
        {
            String[] selectItems;

            if (this.rawListView.CheckedItems.Count == 0)
            {
                selectItems = new String[1];
                selectItems[0] = "*";
            }
            else
            {
                selectItems = new String[this.rawListView.CheckedItems.Count];
                int i = 0;
                foreach (ListViewItem lvi in this.rawListView.CheckedItems)
                {
                    selectItems[i++] = lvi.Text;
                }
            }

            return selectItems;
        }

        private void FillExcel(JObject jo, Range range)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;

                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                range.UnMerge();
                int beginRow = range.Row;
                int beginCol = range.Column;

                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

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

        private String GenerateSql(String db, String table, String[] fields, String from, String to, bool asc, Int32 limitRows)
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("select ").Append(fields[0]);
            for (int i = 1; i < fields.Length; ++i)
            {
                sql.Append(", ").Append(fields[i]);
            }

            sql.Append(" from ").Append(db).Append(".").Append(table).Append(" where _c0 >= '").Append(from).Append("' and _c0 < '").Append(to).Append("'");

            if (asc)
            {
                sql.Append(" order by _c0 asc");
            }

            if (limitRows != 0)
            {
                sql.Append(" limit ").Append(limitRows.ToString());
            }

            return sql.ToString();
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.rawListView = new System.Windows.Forms.ListView();
            this.limitrowsNumericUpDown = new System.Windows.Forms.NumericUpDown();
            this.headsCheck = new System.Windows.Forms.CheckBox();
            this.timestampCheck = new System.Windows.Forms.CheckBox();
            this.label1 = new System.Windows.Forms.Label();
            this.tableLayoutPanel2 = new System.Windows.Forms.TableLayoutPanel();
            this.outputTextbox = new System.Windows.Forms.TextBox();
            this.outputButton = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.tableLayoutPanel3 = new System.Windows.Forms.TableLayoutPanel();
            this.import = new System.Windows.Forms.Button();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.tableLayoutPanel4 = new System.Windows.Forms.TableLayoutPanel();
            this.inputTextBox = new System.Windows.Forms.TextBox();
            this.inputButton = new System.Windows.Forms.Button();
            this.ascCheck = new System.Windows.Forms.CheckBox();
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.toTimePicker = new System.Windows.Forms.DateTimePicker();
            this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
            this.tableLayoutPanel5 = new System.Windows.Forms.TableLayoutPanel();
            this.showButton = new System.Windows.Forms.Button();
            this.label11 = new System.Windows.Forms.Label();
            this.tableLayoutPanel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.limitrowsNumericUpDown)).BeginInit();
            this.tableLayoutPanel2.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.tableLayoutPanel4.SuspendLayout();
            this.tableLayoutPanel5.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.rawListView, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.limitrowsNumericUpDown, 0, 10);
            this.tableLayoutPanel1.Controls.Add(this.headsCheck, 0, 12);
            this.tableLayoutPanel1.Controls.Add(this.timestampCheck, 0, 13);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 16);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel2, 0, 17);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 15);
            this.tableLayoutPanel1.Controls.Add(this.label3, 0, 11);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 18);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 19);
            this.tableLayoutPanel1.Controls.Add(this.label5, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.label6, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel4, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.ascCheck, 0, 14);
            this.tableLayoutPanel1.Controls.Add(this.label8, 0, 5);
            this.tableLayoutPanel1.Controls.Add(this.label9, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.label10, 0, 9);
            this.tableLayoutPanel1.Controls.Add(this.toTimePicker, 0, 8);
            this.tableLayoutPanel1.Controls.Add(this.fromTimePicker, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel5, 0, 3);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 21;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
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
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 900);
            this.tableLayoutPanel1.TabIndex = 2;
            // 
            // rawListView
            // 
            this.rawListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rawListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawListView.Location = new System.Drawing.Point(18, 156);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(464, 120);
            this.rawListView.TabIndex = 25;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // limitrowsNumericUpDown
            // 
            this.limitrowsNumericUpDown.Dock = System.Windows.Forms.DockStyle.Fill;
            this.limitrowsNumericUpDown.Increment = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            this.limitrowsNumericUpDown.Location = new System.Drawing.Point(18, 436);
            this.limitrowsNumericUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.limitrowsNumericUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.limitrowsNumericUpDown.Name = "limitrowsNumericUpDown";
            this.limitrowsNumericUpDown.Size = new System.Drawing.Size(464, 29);
            this.limitrowsNumericUpDown.TabIndex = 23;
            this.limitrowsNumericUpDown.Value = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.headsCheck.Location = new System.Drawing.Point(18, 499);
            this.headsCheck.Name = "headsCheck";
            this.headsCheck.Size = new System.Drawing.Size(464, 26);
            this.headsCheck.TabIndex = 1;
            this.headsCheck.Text = "show heads";
            this.headsCheck.UseVisualStyleBackColor = true;
            // 
            // timestampCheck
            // 
            this.timestampCheck.AutoSize = true;
            this.timestampCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.timestampCheck.Location = new System.Drawing.Point(18, 531);
            this.timestampCheck.Name = "timestampCheck";
            this.timestampCheck.Size = new System.Drawing.Size(464, 26);
            this.timestampCheck.TabIndex = 2;
            this.timestampCheck.Text = "display as timestamp";
            this.timestampCheck.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 623);
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
            this.tableLayoutPanel2.Location = new System.Drawing.Point(18, 651);
            this.tableLayoutPanel2.Name = "tableLayoutPanel2";
            this.tableLayoutPanel2.RowCount = 1;
            this.tableLayoutPanel2.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel2.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel2.TabIndex = 5;
            // 
            // outputTextbox
            // 
            this.outputTextbox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.outputTextbox.Location = new System.Drawing.Point(3, 3);
            this.outputTextbox.Name = "outputTextbox";
            this.outputTextbox.Size = new System.Drawing.Size(342, 29);
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
            this.label2.Location = new System.Drawing.Point(18, 595);
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
            this.label3.Location = new System.Drawing.Point(18, 471);
            this.label3.Margin = new System.Windows.Forms.Padding(3);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(464, 22);
            this.label3.TabIndex = 7;
            this.label3.Text = "Options";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label4.Location = new System.Drawing.Point(18, 692);
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
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 720);
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
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label5.Location = new System.Drawing.Point(18, 18);
            this.label5.Margin = new System.Windows.Forms.Padding(3);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(464, 22);
            this.label5.TabIndex = 10;
            this.label5.Text = "Input";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label6.Location = new System.Drawing.Point(18, 46);
            this.label6.Margin = new System.Windows.Forms.Padding(3);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(464, 22);
            this.label6.TabIndex = 11;
            this.label6.Text = "name of table or stable";
            // 
            // tableLayoutPanel4
            // 
            this.tableLayoutPanel4.ColumnCount = 2;
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel4.Controls.Add(this.inputTextBox, 0, 0);
            this.tableLayoutPanel4.Controls.Add(this.inputButton, 1, 0);
            this.tableLayoutPanel4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel4.Location = new System.Drawing.Point(18, 74);
            this.tableLayoutPanel4.Name = "tableLayoutPanel4";
            this.tableLayoutPanel4.RowCount = 1;
            this.tableLayoutPanel4.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel4.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel4.TabIndex = 12;
            // 
            // inputTextBox
            // 
            this.inputTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputTextBox.Location = new System.Drawing.Point(3, 3);
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(342, 29);
            this.inputTextBox.TabIndex = 0;
            // 
            // inputButton
            // 
            this.inputButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.inputButton.Location = new System.Drawing.Point(351, 3);
            this.inputButton.Name = "inputButton";
            this.inputButton.Size = new System.Drawing.Size(110, 29);
            this.inputButton.TabIndex = 1;
            this.inputButton.Text = "Select";
            this.inputButton.UseVisualStyleBackColor = true;
            this.inputButton.Click += new System.EventHandler(this.InputButton_Click);
            // 
            // ascCheck
            // 
            this.ascCheck.AutoSize = true;
            this.ascCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ascCheck.Location = new System.Drawing.Point(18, 563);
            this.ascCheck.Name = "ascCheck";
            this.ascCheck.Size = new System.Drawing.Size(464, 26);
            this.ascCheck.TabIndex = 13;
            this.ascCheck.Text = "ascending by time";
            this.ascCheck.UseVisualStyleBackColor = true;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label8.Location = new System.Drawing.Point(18, 282);
            this.label8.Margin = new System.Windows.Forms.Padding(3);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(464, 22);
            this.label8.TabIndex = 18;
            this.label8.Text = "timestamp from";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(18, 345);
            this.label9.Margin = new System.Windows.Forms.Padding(3);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(130, 22);
            this.label9.TabIndex = 19;
            this.label9.Text = "timestamp to";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(18, 408);
            this.label10.Margin = new System.Windows.Forms.Padding(3);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(110, 22);
            this.label10.TabIndex = 20;
            this.label10.Text = "limit rows";
            // 
            // toTimePicker
            // 
            this.toTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.toTimePicker.Dock = System.Windows.Forms.DockStyle.Fill;
            this.toTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.toTimePicker.Location = new System.Drawing.Point(18, 373);
            this.toTimePicker.Name = "toTimePicker";
            this.toTimePicker.Size = new System.Drawing.Size(464, 29);
            this.toTimePicker.TabIndex = 21;
            // 
            // fromTimePicker
            // 
            this.fromTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.fromTimePicker.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fromTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.fromTimePicker.Location = new System.Drawing.Point(18, 310);
            this.fromTimePicker.Name = "fromTimePicker";
            this.fromTimePicker.Size = new System.Drawing.Size(464, 29);
            this.fromTimePicker.TabIndex = 22;
            // 
            // tableLayoutPanel5
            // 
            this.tableLayoutPanel5.ColumnCount = 2;
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel5.Controls.Add(this.showButton, 0, 0);
            this.tableLayoutPanel5.Controls.Add(this.label11, 0, 0);
            this.tableLayoutPanel5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel5.Location = new System.Drawing.Point(18, 115);
            this.tableLayoutPanel5.Name = "tableLayoutPanel5";
            this.tableLayoutPanel5.RowCount = 1;
            this.tableLayoutPanel5.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel5.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel5.TabIndex = 24;
            // 
            // showButton
            // 
            this.showButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.showButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.showButton.Location = new System.Drawing.Point(351, 3);
            this.showButton.Name = "showButton";
            this.showButton.Size = new System.Drawing.Size(110, 29);
            this.showButton.TabIndex = 2;
            this.showButton.Text = "Show";
            this.showButton.UseVisualStyleBackColor = true;
            this.showButton.Click += new System.EventHandler(this.ShowButton_Click);
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label11.Location = new System.Drawing.Point(0, 6);
            this.label11.Margin = new System.Windows.Forms.Padding(0, 6, 3, 3);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(345, 26);
            this.label11.TabIndex = 1;
            this.label11.Text = "columns";
            // 
            // TDDetailsForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 900);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDDetailsForm";
            this.Size = new System.Drawing.Size(500, 900);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.limitrowsNumericUpDown)).EndInit();
            this.tableLayoutPanel2.ResumeLayout(false);
            this.tableLayoutPanel2.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.tableLayoutPanel4.ResumeLayout(false);
            this.tableLayoutPanel4.PerformLayout();
            this.tableLayoutPanel5.ResumeLayout(false);
            this.tableLayoutPanel5.PerformLayout();
            this.ResumeLayout(false);

        }
    }
}

using System.Runtime.InteropServices;
using System;
using System.Linq;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Collections;
using System.Threading;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDSliceForm : TDControl
    {
        private TableLayoutPanel tableLayoutPanel1;
        private CheckBox headsCheck;
        private CheckBox timestampCheck;
        private Label label1;
        private TableLayoutPanel tableLayoutPanel2;
        private TextBox outputTextbox;
        private Button outputButton;
        private Label label2;
        private Label label3;
        private Label label4;
        private TableLayoutPanel tableLayoutPanel3;
        private Button import;
        private Label label5;
        private Label label8;
        private Label label9;
        private DateTimePicker fromTimePicker;
        private ComboBox fillMethodCombox;
        private Label label10;
        private NumericUpDown fillValueNumericUpDown;
        private TableLayoutPanel tableLayoutPanel5;
        private Label label11;
        private ListView rawListView;
        private Button showButton;
        private TableLayoutPanel tableLayoutPanel6;
        private Button inputButton;
        private Label label6;
        private RichTextBox inputTextBox;
        public Label TheLabel;
        public TDSliceForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();

            foreach (String sliceFillMethod in TDFactory.Util.fillMethods)
            {
                this.fillMethodCombox.Items.Add(sliceFillMethod);
            }
            this.fillValueNumericUpDown.Value = 0;
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = TDFactory.Persist.sliceInput;
            this.FillListViewInitial();

            if (TDFactory.Persist.sliceTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                fromTimePicker.Value = begin;
            }
            else
            {
                DateTime begin = new DateTime(TDFactory.Persist.sliceTimestamp);
                fromTimePicker.Value = begin;
            }

            this.headsCheck.Checked = TDFactory.Persist.sliceShowHeads;
            this.timestampCheck.Checked = TDFactory.Persist.sliceDisplayAsTimestamp;

            this.fillMethodCombox.SelectedIndex = TDFactory.Persist.sliceFillMethodIndex;
            if (TDFactory.Persist.sliceFillMethodValue >= -100000 && TDFactory.Persist.sliceFillMethodValue <= 100000)
                this.fillValueNumericUpDown.Value = (int)TDFactory.Persist.sliceFillMethodValue;

            this.outputTextbox.Text = TDFactory.Persist.sliceOutput;

            this.fillMethodCombox.SelectedIndexChanged += new System.EventHandler(this.FillMethodCombox_SelectedIndexChanged);
            this.UpdateControls();
        }

        public override void Save()
        {
            TDFactory.Persist.sliceInput = this.inputTextBox.Text;
            TDFactory.Persist.sliceTimestamp = fromTimePicker.Value.Ticks;
            TDFactory.Persist.sliceFillMethodIndex = this.fillMethodCombox.SelectedIndex;
            TDFactory.Persist.sliceFillMethodValue = (double)this.fillValueNumericUpDown.Value;

            TDFactory.Persist.sliceShowHeads = this.headsCheck.Checked;
            TDFactory.Persist.sliceDisplayAsTimestamp = this.timestampCheck.Checked;

            TDFactory.Persist.sliceSelectFields.Clear();
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                TDFactory.Persist.sliceSelectFields.Add(lvi.Text);
            }

            TDFactory.Persist.sliceOutput = this.outputTextbox.Text;
        }
        
        private void InputButton_Click(object sender, EventArgs e)
        {
            this.inputTextBox.Text = TDFactory.Excel.GetSelectionRangesValue();
            if (this.inputTextBox.Text != TDFactory.Util.TD_TABLE_EMPTY_SELECTION) this.FillListView();
        }

        private void ShowButton_Click(object sender, EventArgs e)
        {
            if (this.inputTextBox.Text != TDFactory.Util.TD_TABLE_EMPTY_SELECTION) this.FillListView();
        }

        private void FillMethodCombox_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
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
                TDFactory.Util.ShowError(TDFactory.Util.TD_OUTPUT_NOT_SELECT);
                return;
            }

            TDTable table = TDFactory.Util.ExplainTables(this.inputTextBox.Text.ToLower()).Result;
            if (table.error != String.Empty)
            {
                TDFactory.Util.ShowError(table.error);
                return;
            }

            String[] selectedFields = this.GetListViewCheckedItems();
            String from = fromTimePicker.Text;
            String fillMethod = TDFactory.Util.fillMethods[this.fillMethodCombox.SelectedIndex];
            double fillValue = (double)this.fillValueNumericUpDown.Value;

            TDHttpTimestampType displayAsTimestamp = this.timestampCheck.Checked ? TDHttpTimestampType.TD_SHOW_TIMESTSAMP : TDHttpTimestampType.TD_SHOW_TIME_STRING;

            if (table.tables.Length <= 1)
            {
                String sql = this.GenerateSql(table, selectedFields, from, fillMethod, fillValue, 0, table.tables.Length);
                TDHttpReturn resp = TDFactory.Util.DoRequest(sql, displayAsTimestamp).Result;
                if (resp.jo != null)
                {
                    ArrayList jobjs = new ArrayList();
                    jobjs.Add(resp.jo);
                    TDFactory.StartUpdate();
                    this.FillExcel(jobjs, outputRange, table.tables);
                    TDFactory.EndUpdate();
                }
                else
                {
                    TDFactory.Util.ShowError(resp.error);
                }
            }
            else
            {
                if (table.tables.Length > 3 * TDFactory.Util.TD_MAX_SQL_COUNT)
                {
                    ArrayList jobjs = new ArrayList();
                    ArrayList results = new ArrayList();

                    Thread[] threads = new Thread[TDFCalculateForm.threadNum];
                    for (int index = 0; index < TDFCalculateForm.threadNum; index++)
                    {
                        ParameterizedThreadStart start = new ParameterizedThreadStart(ParallelExecuting);
                        TDSliceParameter parameter = new TDSliceParameter();
                        parameter.threadIndex = index;
                        parameter.table = table;
                        parameter.selectedFields = selectedFields;
                        parameter.from = from;
                        parameter.fillMethod = fillMethod;
                        parameter.fillValue = fillValue;
                        parameter.displayAsTimestamp = displayAsTimestamp;
                        parameter.results = results;

                        threads[index] = new Thread(start);
                        threads[index].Start(parameter);
                    }

                    foreach (Thread thread in threads)
                    {
                        thread.Join();
                    }

                    if (results.Count == 0)
                    {
                        TDFactory.Util.ShowError("exception occured while schedule sqls");
                        return;
                    }
                    else
                    {
                        for (int i = 0; i < results.Count; ++i)
                        {
                            TDHttpReturn resp = results[i] as TDHttpReturn;
                            if (resp.jo != null)
                            {
                                jobjs.Add(resp.jo);
                            }
                            else
                            {
                                TDFactory.Util.ShowError(resp.error);
                                return;
                            }
                        }
                    }

                    if (jobjs.Count > 0)
                    {
                        TDFactory.StartUpdate();
                        this.FillExcel(jobjs, outputRange, table.tables);
                        TDFactory.EndUpdate();
                    }
                }
                else
                {
                    ArrayList jobjs = new ArrayList();

                    for (int i = 0; i < table.tables.Length; i += TDFactory.Util.TD_MAX_SQL_COUNT)
                    {
                        String sql = this.GenerateSql(table, selectedFields, from, fillMethod, fillValue, i, i + TDFactory.Util.TD_MAX_SQL_COUNT);
                        
                        TDHttpReturn resp = TDFactory.Util.DoRequest(sql, displayAsTimestamp).Result;
                        if (resp.jo != null)
                        {
                            jobjs.Add(resp.jo);
                        }
                        else
                        {
                            TDFactory.Util.ShowError(resp.error);
                            return;
                        }
                    }

                    TDFactory.StartUpdate();
                    this.FillExcel(jobjs, outputRange, table.tables);
                    TDFactory.EndUpdate();
                }
            }
        }

        public void ParallelExecuting(object obj)
        {
            TDSliceParameter para = obj as TDSliceParameter;
            int threadIndex = para.threadIndex;
            int threadNum = TDFCalculateForm.threadNum;

            for (int i = threadIndex * TDFactory.Util.TD_MAX_SQL_COUNT;
                i < para.table.tables.Length;
                i += (threadNum * TDFactory.Util.TD_MAX_SQL_COUNT))
            {
                String sql = this.GenerateSql(para.table, para.selectedFields, para.from, para.fillMethod, para.fillValue, i, i + TDFactory.Util.TD_MAX_SQL_COUNT);

                TDHttpReturn resp = TDFactory.Util.DoRequest(sql, para.displayAsTimestamp).Result;
                para.results.Add(resp);
                if (resp.error != String.Empty) return;
            }
        }

        private void FillListViewInitial()
        {
            this.ClearListView();
            foreach (String field in TDFactory.Persist.sliceSelectFields)
            {
                ListViewItem lvi = new ListViewItem();
                lvi.Text = field;
                lvi.Checked = true;
                this.rawListView.Items.Add(lvi);
            }
        }

        private void FillListView()
        {
            this.ClearListView();

            TDTable table = TDFactory.Util.ExplainTables(this.inputTextBox.Text).Result;
            if (table.error != String.Empty)
            {
                TDFactory.Util.ShowError(table.error);
                return;
            }

            String sql = TDFactory.Util.GenerateDescribeSql(table);
            TDHttpReturn resp = TDFactory.Util.DoRequest(sql, TDHttpTimestampType.TD_SHOW_TIME_STRING).Result;

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

                bool isMetrics = (table.stable != String.Empty) && ((table.tables.Length != 1) || table.table == table.stable);
                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String field = dataCols.GetValue(0).ToString();
                    String fieldType = dataCols.GetValue(1).ToString();
                    String tag = dataCols.GetValue(3).ToString();

                    if (fieldType == "BINARY" || fieldType == "NCHAR")
                    {
                        fieldType = fieldType + "(" + dataCols.GetValue(2).ToString() + ")";
                    }

                    if (tag != ""/* && !isMetrics*/)
                    {
                        continue;
                    }

                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = field;
                    lvi.SubItems.Add(fieldType.ToLower());
                    this.rawListView.Items.Add(lvi);
                }

                if (isMetrics)
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

        private void FillExcel(ArrayList jObjs, Range range, String[] tableArray)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;

                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                range.UnMerge();
                int beginRow = range.Row;
                int beginCol = range.Column;

                JObject jo = jObjs[0] as JObject;
                Array heads = jo.GetValue("head").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = 0;
                for (int i = 0; i < jObjs.Count; ++i)
                {
                    JObject jObj = jObjs[i] as JObject;
                    Array datas = jObj.GetValue("data").ToArray();
                    dataLength += datas.GetLength(0);
                }

                int beginOffset = 0;
                int maxRowLength = Math.Max(tableArray.Length, showHeads ? dataLength + 1 : dataLength) + 100;
                int[] rowArray = TDFactory.Excel.GetUnHiddenRows(beginRow, maxRowLength);
                int[] colArray = TDFactory.Excel.GetUnHiddenColumns(beginCol, headLength);

                if (showHeads)
                {
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[beginRow, col + beginCol].Value2 = heads.GetValue(col).ToString();
                    }
                    beginOffset++;
                }

                if (tableArray.Length > 1)
                {
                    Hashtable resultHash = new Hashtable();
                    for (int i = 0; i < jObjs.Count; ++i)
                    {
                        JObject jObj = jObjs[i] as JObject;
                        Array datas = jObj.GetValue("data").ToArray();
                        dataLength = datas.GetLength(0);
                        for (int row = 0; row < dataLength; ++row)
                        {
                            Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                            String groupByName = dataCols.GetValue(headLength - 1).ToString();
                            resultHash.Add(groupByName, dataCols);
                        }
                    }

                    for (int row = 0; row < tableArray.Length; ++row)
                    {
                        String tableName = tableArray[row];
                        Object hashValue = resultHash[tableName];

                        if (row >= maxRowLength) break;

                        if (hashValue == null)
                        {
                            activeWorksheet.Cells[rowArray[row + beginOffset], colArray[0]].Value2 = TDFactory.Util.ErrorPrefix + "no result of " + tableName;
                        }
                        else
                        {
                            Array dataCols = hashValue as Array;
                            for (int col = 0; col < headLength; ++col)
                            {
                                activeWorksheet.Cells[rowArray[row + beginOffset], colArray[col]].Value2 = dataCols.GetValue(col).ToString();
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < jObjs.Count; ++i)
                    {
                        JObject jObj = jObjs[i] as JObject;
                        Array datas = jObj.GetValue("data").ToArray();
                        dataLength = datas.GetLength(0);
                        for (int row = 0; row < dataLength; ++row)
                        {
                            Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                            for (int col = 0; col < headLength; ++col)
                            {
                                activeWorksheet.Cells[rowArray[row + beginOffset], colArray[col]].Value2 = dataCols.GetValue(col).ToString();
                            }
                        }
                        beginOffset += dataLength;
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

        private String GenerateSql(TDTable table, String[] fields, String from, String fillMethod, double fillValue, int begin, int end)
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("select interp(").Append(fields[0]);
            for (int i = 1; i < fields.Length; ++i)
            {
                sql.Append(", ").Append(fields[i]);
            }
            sql.Append(") from ");

            if (table.stable == String.Empty)
            {
                sql.Append(TDFactory.Persist.connectDB).Append(".").Append(table.table).Append(" where _c0 = '").Append(from).Append("'");
            }
            else if (table.tables.Length == 1)
            {
                sql.Append(TDFactory.Persist.connectDB).Append(".").Append(table.table).Append(" where _c0 = '").Append(from).Append("'");
            }
            else
            {
                sql.Append(TDFactory.Persist.connectDB).Append(".").Append(table.stable).Append(" where tbname in(");
                sql.Append("'").Append(table.tables[begin]).Append("'");
                if (end > table.tables.Length) end = table.tables.Length;
                for (int i = begin+1; i < end; ++i)
                {
                    sql.Append(",'").Append(table.tables[i]).Append("'");
                }
                sql.Append(") and _c0='").Append(from).Append("'");
            }

            if (fillMethod != "value")
            {
                sql.Append(" fill(").Append(fillMethod).Append(")");
            }
            else
            {
                sql.Append(" fill(").Append(fillMethod).Append(",").Append(fillValue).Append(")");
            }

            if (table.stable == String.Empty)
            { }
            else if (table.tables.Length == 1)
            {
                if (table.stable == table.table)
                    sql.Append(" group by tbname");
            }
            else
            {
                sql.Append(" group by tbname");
            }

            return sql.ToString();
        }

        private void UpdateControls()
        {
            this.fillValueNumericUpDown.Enabled = this.fillMethodCombox.SelectedIndex == 2;
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.rawListView = new System.Windows.Forms.ListView();
            this.fillValueNumericUpDown = new System.Windows.Forms.NumericUpDown();
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
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
            this.fillMethodCombox = new System.Windows.Forms.ComboBox();
            this.label10 = new System.Windows.Forms.Label();
            this.tableLayoutPanel5 = new System.Windows.Forms.TableLayoutPanel();
            this.showButton = new System.Windows.Forms.Button();
            this.label11 = new System.Windows.Forms.Label();
            this.tableLayoutPanel6 = new System.Windows.Forms.TableLayoutPanel();
            this.inputButton = new System.Windows.Forms.Button();
            this.label6 = new System.Windows.Forms.Label();
            this.inputTextBox = new System.Windows.Forms.RichTextBox();
            this.tableLayoutPanel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).BeginInit();
            this.tableLayoutPanel2.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.tableLayoutPanel5.SuspendLayout();
            this.tableLayoutPanel6.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.rawListView, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.fillValueNumericUpDown, 0, 10);
            this.tableLayoutPanel1.Controls.Add(this.headsCheck, 0, 12);
            this.tableLayoutPanel1.Controls.Add(this.timestampCheck, 0, 13);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 15);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel2, 0, 17);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 14);
            this.tableLayoutPanel1.Controls.Add(this.label3, 0, 11);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 18);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 19);
            this.tableLayoutPanel1.Controls.Add(this.label5, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.label8, 0, 5);
            this.tableLayoutPanel1.Controls.Add(this.label9, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.fromTimePicker, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.fillMethodCombox, 0, 8);
            this.tableLayoutPanel1.Controls.Add(this.label10, 0, 9);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel5, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel6, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.inputTextBox, 0, 2);
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
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 1000);
            this.tableLayoutPanel1.TabIndex = 4;
            // 
            // rawListView
            // 
            this.rawListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rawListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawListView.Location = new System.Drawing.Point(18, 209);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(464, 120);
            this.rawListView.TabIndex = 41;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // fillValueNumericUpDown
            // 
            this.fillValueNumericUpDown.DecimalPlaces = 2;
            this.fillValueNumericUpDown.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fillValueNumericUpDown.Location = new System.Drawing.Point(18, 484);
            this.fillValueNumericUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.fillValueNumericUpDown.Minimum = new decimal(new int[] {
            100000,
            0,
            0,
            -2147483648});
            this.fillValueNumericUpDown.Name = "fillValueNumericUpDown";
            this.fillValueNumericUpDown.Size = new System.Drawing.Size(464, 29);
            this.fillValueNumericUpDown.TabIndex = 39;
            this.fillValueNumericUpDown.Value = new decimal(new int[] {
            100000,
            0,
            0,
            -2147483648});
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.headsCheck.Location = new System.Drawing.Point(18, 547);
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
            this.timestampCheck.Location = new System.Drawing.Point(18, 579);
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
            this.label1.Location = new System.Drawing.Point(18, 639);
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
            this.tableLayoutPanel2.Location = new System.Drawing.Point(18, 667);
            this.tableLayoutPanel2.Name = "tableLayoutPanel2";
            this.tableLayoutPanel2.RowCount = 1;
            this.tableLayoutPanel2.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel2.Size = new System.Drawing.Size(464, 33);
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
            this.outputButton.Size = new System.Drawing.Size(113, 27);
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
            this.label2.Location = new System.Drawing.Point(18, 611);
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
            this.label3.Location = new System.Drawing.Point(18, 519);
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
            this.label4.Location = new System.Drawing.Point(18, 706);
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
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 734);
            this.tableLayoutPanel3.Name = "tableLayoutPanel3";
            this.tableLayoutPanel3.RowCount = 1;
            this.tableLayoutPanel3.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel3.Size = new System.Drawing.Size(464, 33);
            this.tableLayoutPanel3.TabIndex = 9;
            // 
            // import
            // 
            this.import.Dock = System.Windows.Forms.DockStyle.Fill;
            this.import.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.import.Location = new System.Drawing.Point(351, 3);
            this.import.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.import.Name = "import";
            this.import.Size = new System.Drawing.Size(113, 27);
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
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label8.Location = new System.Drawing.Point(18, 335);
            this.label8.Margin = new System.Windows.Forms.Padding(3);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(464, 22);
            this.label8.TabIndex = 18;
            this.label8.Text = "timestamp";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(18, 398);
            this.label9.Margin = new System.Windows.Forms.Padding(3);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(210, 22);
            this.label9.TabIndex = 19;
            this.label9.Text = "interpolation method";
            // 
            // fromTimePicker
            // 
            this.fromTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.fromTimePicker.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fromTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.fromTimePicker.Location = new System.Drawing.Point(18, 363);
            this.fromTimePicker.Name = "fromTimePicker";
            this.fromTimePicker.Size = new System.Drawing.Size(464, 29);
            this.fromTimePicker.TabIndex = 22;
            // 
            // fillMethodCombox
            // 
            this.fillMethodCombox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fillMethodCombox.FormattingEnabled = true;
            this.fillMethodCombox.Location = new System.Drawing.Point(18, 426);
            this.fillMethodCombox.Name = "fillMethodCombox";
            this.fillMethodCombox.Size = new System.Drawing.Size(464, 30);
            this.fillMethodCombox.TabIndex = 54;
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label10.Location = new System.Drawing.Point(18, 459);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(464, 22);
            this.label10.TabIndex = 25;
            this.label10.Text = "interpolation value";
            // 
            // tableLayoutPanel5
            // 
            this.tableLayoutPanel5.ColumnCount = 2;
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel5.Controls.Add(this.showButton, 0, 0);
            this.tableLayoutPanel5.Controls.Add(this.label11, 0, 0);
            this.tableLayoutPanel5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel5.Location = new System.Drawing.Point(18, 168);
            this.tableLayoutPanel5.Name = "tableLayoutPanel5";
            this.tableLayoutPanel5.RowCount = 1;
            this.tableLayoutPanel5.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel5.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel5.TabIndex = 40;
            // 
            // showButton
            // 
            this.showButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.showButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.showButton.Location = new System.Drawing.Point(351, 3);
            this.showButton.Name = "showButton";
            this.showButton.Size = new System.Drawing.Size(110, 29);
            this.showButton.TabIndex = 3;
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
            this.label11.TabIndex = 2;
            this.label11.Text = "columns";
            // 
            // tableLayoutPanel6
            // 
            this.tableLayoutPanel6.ColumnCount = 2;
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel6.Controls.Add(this.inputButton, 1, 0);
            this.tableLayoutPanel6.Controls.Add(this.label6, 0, 0);
            this.tableLayoutPanel6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel6.Location = new System.Drawing.Point(18, 46);
            this.tableLayoutPanel6.Name = "tableLayoutPanel6";
            this.tableLayoutPanel6.RowCount = 1;
            this.tableLayoutPanel6.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel6.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel6.TabIndex = 42;
            // 
            // inputButton
            // 
            this.inputButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.inputButton.Location = new System.Drawing.Point(351, 3);
            this.inputButton.Name = "inputButton";
            this.inputButton.Size = new System.Drawing.Size(110, 29);
            this.inputButton.TabIndex = 2;
            this.inputButton.Text = "Select";
            this.inputButton.UseVisualStyleBackColor = true;
            this.inputButton.Click += new System.EventHandler(this.InputButton_Click);
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label6.Location = new System.Drawing.Point(3, 6);
            this.label6.Margin = new System.Windows.Forms.Padding(3, 6, 3, 3);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(342, 26);
            this.label6.TabIndex = 0;
            this.label6.Text = "name of table or stable";
            // 
            // inputTextBox
            // 
            this.inputTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputTextBox.Location = new System.Drawing.Point(18, 87);
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(464, 75);
            this.inputTextBox.TabIndex = 43;
            this.inputTextBox.Text = "";
            // 
            // TDSliceForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 1000);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDSliceForm";
            this.Size = new System.Drawing.Size(500, 1000);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).EndInit();
            this.tableLayoutPanel2.ResumeLayout(false);
            this.tableLayoutPanel2.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.tableLayoutPanel5.ResumeLayout(false);
            this.tableLayoutPanel5.PerformLayout();
            this.tableLayoutPanel6.ResumeLayout(false);
            this.tableLayoutPanel6.PerformLayout();
            this.ResumeLayout(false);

        }

        internal class TDSliceParameter
        {
            public int threadIndex;
            public TDTable table;
            public String[] selectedFields;
            public String from;
            public String fillMethod;
            public double fillValue;
            public TDHttpTimestampType displayAsTimestamp;
            public ArrayList results;
        }
    }
}

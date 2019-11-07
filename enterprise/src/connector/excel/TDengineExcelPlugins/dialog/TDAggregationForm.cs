using System.Runtime.InteropServices;
using System;
using System.Linq;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;
using System.Collections;
using System.Threading.Tasks;
using System.Text;
using System.ComponentModel;
using System.Threading;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDAggregationForm : TDControl
    {
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
        private Label label8;
        private Label label9;
        private DateTimePicker toTimePicker;
        private DateTimePicker fromTimePicker;
        private TableLayoutPanel tableLayoutPanel1;
        private TableLayoutPanel tableLayoutPanel5;
        private CheckBox groupbyCheckBox;
        private TableLayoutPanel tableLayoutPanel6;
        private CheckBox intervalCheckBox;
        private ComboBox groupbyComboBox;
        private ComboBox intervalTimeUnitComboBox;
        private NumericUpDown intervalTimeNumericUpDown;
        private Label label5;
        private TableLayoutPanel tableLayoutPanel9;
        private Button inputButton;
        private Label label6;
        private TableLayoutPanel tableLayoutPanel11;
        private Button unselectButton;
        private Label label12;
        private ListView rawListView;
        private RichTextBox inputTextBox;
        private TableLayoutPanel tableLayoutPanel4;
        private Button showButton;
        private Label label7;
        private TableLayoutPanel tableLayoutPanel10;
        private ComboBox functionCombox;
        private Label label11;
        private ListView aggListView;
        private TableLayoutPanel tableLayoutPanel7;
        private ComboBox fillMethodCombox;
        private NumericUpDown fillValueNumericUpDown;
        public Label TheLabel;
        public TDAggregationForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();
            
            foreach (String aggFunction in TDFactory.Util.aggFunctions)
            {
                this.functionCombox.Items.Add(aggFunction);
            }
            this.functionCombox.SelectedIndex = 0;

            foreach (String intervalTimeUnit in TDFactory.Util.intervalTimeUnits)
            {
                this.intervalTimeUnitComboBox.Items.Add(intervalTimeUnit);
            }
            this.intervalTimeUnitComboBox.SelectedIndex = 0;

            foreach (String fillMethod in TDFactory.Util.fillMethods)
            {
                this.fillMethodCombox.Items.Add(fillMethod);
            }
            this.fillMethodCombox.SelectedIndex = 0;
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = TDFactory.Persist.aggInput;
            this.FillColumnsListViewInitial();
            this.FillAggListViewInitial();
            
            if (TDFactory.Persist.aggFromTimestamp == 0 && TDFactory.Persist.aggToTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                DateTime end = (DateTime.Now.AddDays(1)).Date;
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }
            else
            {
                DateTime begin = new DateTime(TDFactory.Persist.aggFromTimestamp);
                DateTime end = new DateTime(TDFactory.Persist.aggToTimestamp);
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }

            this.headsCheck.Checked = TDFactory.Persist.aggShowHeads;
            this.timestampCheck.Checked = TDFactory.Persist.aggDisplayAsTimestamp;

            this.groupbyCheckBox.Checked = TDFactory.Persist.aggGroupByCheck;
            this.intervalCheckBox.Checked = TDFactory.Persist.aggIntervalCheck;

            this.UpdateControls();

            this.FillGroubyComboBoxInitial();

            this.intervalTimeNumericUpDown.Value = TDFactory.Persist.aggIntervalTime;
            this.intervalTimeUnitComboBox.SelectedIndex = TDFactory.Persist.aggIntervalTimeUnitIndex;
            this.fillMethodCombox.SelectedIndex = TDFactory.Persist.aggFillMethodIndex;
            this.fillValueNumericUpDown.Value = (int)TDFactory.Persist.aggFillMethodValue;
            this.fillValueNumericUpDown.DecimalPlaces = 2;

            this.outputTextbox.Text = TDFactory.Persist.aggOutput;
            this.functionCombox.SelectedIndexChanged += new System.EventHandler(this.SelectCombox_SelectedIndexChanged);
        }
        
        public override void Save()
        {
            TDFactory.Persist.aggInput = this.inputTextBox.Text;
            TDFactory.Persist.aggSelectFields.Clear();
            foreach (ListViewItem lvi in this.aggListView.Items)
            {
                TDFactory.Persist.aggSelectFields.Add(lvi.Text);
            }

            TDFactory.Persist.aggFromTimestamp = fromTimePicker.Value.Ticks;
            TDFactory.Persist.aggToTimestamp = toTimePicker.Value.Ticks;
            TDFactory.Persist.aggShowHeads = this.headsCheck.Checked;
            TDFactory.Persist.aggDisplayAsTimestamp = this.timestampCheck.Checked;

            TDFactory.Persist.aggGroupByCheck = this.groupbyCheckBox.Checked;
            TDFactory.Persist.aggGroupbyName = this.groupbyComboBox.Text;

            TDFactory.Persist.aggIntervalCheck = this.intervalCheckBox.Checked;
            TDFactory.Persist.aggIntervalTime = Decimal.ToInt32(this.intervalTimeNumericUpDown.Value);
            TDFactory.Persist.aggIntervalTimeUnitIndex = this.intervalTimeUnitComboBox.SelectedIndex;
            TDFactory.Persist.aggFillMethodIndex = this.fillMethodCombox.SelectedIndex;
            TDFactory.Persist.aggFillMethodValue = Decimal.ToDouble(this.fillValueNumericUpDown.Value);
            
            TDFactory.Persist.aggOutput = this.outputTextbox.Text;
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

        private void SelectCombox_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (this.rawListView.CheckedItems.Count == 0)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_FIELDS_SELECT);
                return;
            }

            int aggFuncIndex = this.functionCombox.SelectedIndex;
            if (aggFuncIndex >= TDFactory.Util.aggFunctions.Length)
            {
                this.functionCombox.SelectedIndex = TDFactory.Util.aggFunctions.Length - 1;
                TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_AGG_FUNCTIONS);
                return;
            }
            String aggFuncName = TDFactory.Util.aggFunctions[aggFuncIndex];

            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                String field = lvi.Text;
                String fieldType = lvi.SubItems[1].Text;
                if (this.rawListView.Items[0].Text == field && aggFuncName != "count")
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_SQL_FIRST_COL_ONLY_SUPPROT_COUNT);
                    continue;
                }

                if (fieldType.StartsWith("binary") && aggFuncName != "count")
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_SQL_BINARY_COL_ONLY_SUPPROT_COUNT);
                    continue;
                }

                ListViewItem lvi2 = new ListViewItem();
                lvi2.Text = aggFuncName + "(" + field + ")"; ;
                this.aggListView.Items.Add(lvi2);
            }

            this.inputTextBox.Select(0, 0);
        }

        private void UnselectButton_Click(object sender, EventArgs e)
        {
            this.DeleteAggListSelectedItems();
        }
        
        private void GroupbyCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
        }

        private void IntervalCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
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

            String[] selectedFields = this.GetAggListViewItems();
            if (selectedFields.Length <= 0)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_FIELDS_SELECT);
                return;
            }

            if (selectedFields.Length > 250)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_TOO_MANY_FIELDS_SELECT);
                return;
            }

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

            String from = fromTimePicker.Text;
            String to = toTimePicker.Text;

            bool groupByChecked = this.groupbyCheckBox.Checked;
            String groupByName = this.groupbyComboBox.Text;

            bool intervalCheck = this.intervalCheckBox.Checked;
            int intervalTime = Decimal.ToInt32(this.intervalTimeNumericUpDown.Value);
            String intervalType = TDFactory.Util.intervalTimeUnitTypes[this.intervalTimeUnitComboBox.SelectedIndex];
            String intervalMethod = TDFactory.Util.fillMethods[this.fillMethodCombox.SelectedIndex];
            double intervelMethodValue = Decimal.ToDouble(this.fillValueNumericUpDown.Value);

            if (table.stable != String.Empty)
            {
                foreach (String fields in selectedFields)
                {
                    foreach (String func in TDFactory.Util.aggFunctionsCannotUsedToMetrics)
                    {
                        if (fields.IndexOf(func + "(") != -1)
                        {
                            TDFactory.Util.ShowError(fields + TDFactory.Util.TD_FUNC_CANNOT_USED_TO_METRICS_SURFIX);
                            return;
                        }
                    }
                }
            }

            if (groupByChecked && groupByName.ToLower() != "tbname" && table.stable != String.Empty)
            {
                foreach (String fields in selectedFields)
                {
                    foreach (String func in TDFactory.Util.aggFunctionsMustGroupByTbname)
                    {
                        if (fields.IndexOf(func + "(") != -1)
                        {
                            TDFactory.Util.ShowError(fields + TDFactory.Util.TD_FUNC_MUST_GROUP_BY_TBNAME_SURFIX);
                            return;
                        }
                    }
                }
            }

            TDHttpTimestampType displayAsTimestamp = this.timestampCheck.Checked ? TDHttpTimestampType.TD_SHOW_TIMESTSAMP : TDHttpTimestampType.TD_SHOW_TIME_STRING;
            
            if (table.tables.Length <= 1)
            {
                String sql = this.GenerateSql(table, selectedFields, from, to
                    , groupByChecked, groupByName
                    , intervalCheck, intervalTime, intervalType, intervalMethod, intervelMethodValue, 0, table.tables.Length);

                TDHttpReturn resp = TDFactory.Util.DoRequest(sql, displayAsTimestamp).Result;
                if (resp.jo != null)
                {
                    ArrayList jobjs = new ArrayList();
                    jobjs.Add(resp.jo);
                    TDFactory.StartUpdate();
                    this.FillExcel(jobjs, outputRange, table.tables, false);
                    TDFactory.EndUpdate();
                }
                else
                {
                    TDFactory.Util.ShowError(resp.error);
                }
            }
            else
            {
                bool sortByName = false;
                if (groupByChecked && !intervalCheck && groupByName.ToLower() == "tbname" && table.tables.Length > 1)
                {
                    sortByName = true;
                }

                if (!sortByName)
                {
                    ArrayList jobjs = new ArrayList();

                    String sql = this.GenerateSql(table, selectedFields, from, to
                        , groupByChecked, groupByName
                        , intervalCheck, intervalTime, intervalType, intervalMethod, intervelMethodValue, 0, table.tables.Length);
                    if (table.tables.Length > TDFactory.Util.TD_MAX_SQL_COUNT)
                    {
                        TDFactory.Util.ShowError("Number of tables can't exceed " + TDFactory.Util.TD_MAX_SQL_COUNT + ", except using group by tbname");
                        return;
                    }

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
                    
                    TDFactory.StartUpdate();
                    this.FillExcel(jobjs, outputRange, table.tables, sortByName);
                    TDFactory.EndUpdate();
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
                            TDAggParameter parameter = new TDAggParameter();
                            parameter.threadIndex = index;
                            parameter.table = table;
                            parameter.selectedFields = selectedFields;
                            parameter.from = from;
                            parameter.to = to;
                            parameter.groupByChecked = groupByChecked;
                            parameter.groupByName = groupByName;
                            parameter.intervalCheck = intervalCheck;
                            parameter.intervalTime = intervalTime;
                            parameter.intervalType = intervalType;
                            parameter.intervalMethod = intervalMethod;
                            parameter.intervelMethodValue = intervelMethodValue;
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
                            this.FillExcel(jobjs, outputRange, table.tables, sortByName);
                            TDFactory.EndUpdate();
                        }
                    }
                    else
                    {
                        ArrayList jobjs = new ArrayList();

                        for (int i = 0; i < table.tables.Length; i += TDFactory.Util.TD_MAX_SQL_COUNT)
                        {
                            String sql = this.GenerateSql(table, selectedFields, from, to
                               , groupByChecked, groupByName
                               , intervalCheck, intervalTime, intervalType, intervalMethod, intervelMethodValue, i, i + TDFactory.Util.TD_MAX_SQL_COUNT);

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
                        this.FillExcel(jobjs, outputRange, table.tables, sortByName);
                        TDFactory.EndUpdate();
                    }
                }//sort by name
            }
        }

        public void ParallelExecuting(object obj)
        {
            TDAggParameter para = obj as TDAggParameter;
            int threadIndex = para.threadIndex;
            int threadNum = TDFCalculateForm.threadNum;

            for (int i = threadIndex * TDFactory.Util.TD_MAX_SQL_COUNT;
                i < para.table.tables.Length;
                i += (threadNum * TDFactory.Util.TD_MAX_SQL_COUNT))
            {
                String sql = this.GenerateSql(para.table, para.selectedFields, para.from, para.to
                       , para.groupByChecked, para.groupByName
                       , para.intervalCheck, para.intervalTime, para.intervalType, para.intervalMethod, para.intervelMethodValue, i, i + TDFactory.Util.TD_MAX_SQL_COUNT);

                TDHttpReturn resp = TDFactory.Util.DoRequest(sql, para.displayAsTimestamp).Result;
                para.results.Add(resp);
                if (resp.error != String.Empty) return;
            }
        }

        private void FillGroubyComboBoxInitial()
        {
            this.groupbyComboBox.Items.Clear();
            if (TDFactory.Persist.aggGroupbyName != String.Empty)
            {
                this.groupbyComboBox.Items.Add(TDFactory.Persist.aggGroupbyName);
                this.groupbyComboBox.SelectedIndex = 0;
            }
        }

        private void FillAggListViewInitial()
        {
            this.ClearAggListView();
            foreach (String field in TDFactory.Persist.aggSelectFields)
            {
                ListViewItem lvi = new ListViewItem();
                lvi.Text = field;
                this.aggListView.Items.Add(lvi);
            }
        }

        private void FillColumnsListViewInitial()
        {
            this.ClearColumnsListView();
        }

        private void FillListView()
        {
            String groupbyText = this.groupbyComboBox.Text;
            String[] selectedAggs = this.GetAggListViewItems();
            this.ClearColumnsListView();
            this.ClearAggListView();
            this.groupbyComboBox.Items.Clear();

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
                
                if (table.stable != String.Empty)
                {
                    this.groupbyComboBox.Items.Add("tbname");
                }

                this.rawListView.BeginUpdate();

                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String field = dataCols.GetValue(0).ToString();
                    String fieldType = dataCols.GetValue(1).ToString();
                    String tag = dataCols.GetValue(3).ToString();

                    if (tag != String.Empty)
                    { 
                        if (table.stable != String.Empty)
                        {
                            this.groupbyComboBox.Items.Add(field);
                        }
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

                this.rawListView.EndUpdate();

                if (this.groupbyComboBox.Items.Count != 0)
                {
                    this.groupbyComboBox.SelectedIndex = 0;
                }

                for (int i = 0; i < this.groupbyComboBox.Items.Count; ++i)
                {
                    if (this.groupbyComboBox.Items[i].ToString() == groupbyText)
                    {
                        this.groupbyComboBox.SelectedIndex = i;
                        break;
                    }
                }

                foreach (String agg in selectedAggs)
                {
                    foreach (ListViewItem lvi in this.rawListView.Items)
                    {
                        if (agg.IndexOf("(" + lvi.Text + ")") != -1)
                        {
                            ListViewItem aggLvi = new ListViewItem();
                            aggLvi.Text = agg;
                            this.aggListView.Items.Add(aggLvi);
                        }
                    }
                }
            }
            else
            {
                TDFactory.Util.ShowError(resp.error);
            }
        }

        private void ClearAggListView()
        {
            this.aggListView.BeginUpdate();
            this.aggListView.Clear();

            ColumnHeader ch3 = new ColumnHeader();
            ch3.Text = TDFactory.Util.TD_SELECT_AGG_FUNCTIONS_COLUMN;
            ch3.Width = 400;
            ch3.TextAlign = HorizontalAlignment.Left;
            this.aggListView.Columns.Add(ch3);

            this.aggListView.GridLines = true;
            this.aggListView.FullRowSelect = true;
            this.aggListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.aggListView.CheckBoxes = true;

            this.aggListView.EndUpdate();
        }

        private void ClearColumnsListView()
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
     
        private String[] GetAggListViewItems()
        {
            String[] items;

            if (this.aggListView.Items.Count == 0)
            {
                items = new String[0];
            }
            else
            {
                items = new String[this.aggListView.Items.Count];
                int i = 0;
                foreach (ListViewItem lvi in this.aggListView.Items)
                {
                    items[i++] = lvi.Text;
                }
            }

            return items;
        }

        private void DeleteAggListSelectedItems()
        {
            foreach (ListViewItem lvi in this.aggListView.CheckedItems)
            {
                this.aggListView.Items.Remove(lvi);
            }
        }
        
        //
        // 如果soryByName，那么需将结果集按照tableArray中的表顺序排序，不存在的表输出错误
        //
        private void FillExcel(ArrayList jObjs, Range range, String[] tableArray, bool sortByName)
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
                        activeWorksheet.Cells[rowArray[0], colArray[col]].Value2 = heads.GetValue(col).ToString();
                    }
                    beginOffset++;
                }
                
                if (dataLength == 0)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_DATA_FROM_SERVER_EMPTY);
                    return;
                }

                if (sortByName)
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
            }
            catch (Exception e)
            {
                TDFactory.Util.ShowException(e);
            }
            finally { }
        }

        private String GenerateSql(TDTable table, String[] fields, String from, String to
             , bool groupByChecked, String groupByName
             , bool intervalCheck, Int32 intervalTime, String intervalType, String intervalMethod, Double intervelMethodValue
             , int begin, int end)
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("select ").Append(fields[0]);
            for (int i = 1; i < fields.Length; ++i)
            {
                sql.Append(", ").Append(fields[i]);
            }
            sql.Append(" from ");

            if (table.stable == String.Empty)
            {
                sql.Append(TDFactory.Persist.connectDB).Append(".").Append(table.table).Append(" where _c0 >= '").Append(from).Append("' and _c0 < '").Append(to).Append("'");
            }
            else if (table.tables.Length == 1)
            {
                sql.Append(TDFactory.Persist.connectDB).Append(".").Append(table.table).Append(" where _c0 >= '").Append(from).Append("' and _c0 < '").Append(to).Append("'");
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
                sql.Append(") and _c0 >= '").Append(from).Append("' and _c0 < '").Append(to).Append("'");
            }

            if (intervalCheck)
            {
                sql.Append(" interval(").Append(intervalTime.ToString()).Append(intervalType).Append(")");
                if (intervalMethod != "value")
                {
                    sql.Append(" fill(").Append(intervalMethod).Append(")");
                }
                else
                {
                    sql.Append(" fill(").Append(intervalMethod).Append(", ").Append(intervelMethodValue.ToString()).Append(")");
                }
            }

            if (groupByChecked && groupByName != String.Empty)
            {
                sql.Append(" group by ").Append(groupByName);
            }

            return sql.ToString();
        }

        private void UpdateControls()
        {
            this.groupbyComboBox.Enabled = this.groupbyCheckBox.Checked;
            this.intervalTimeNumericUpDown.Enabled = this.intervalCheckBox.Checked;
            this.intervalTimeUnitComboBox.Enabled = this.intervalCheckBox.Checked;
            this.fillMethodCombox.Enabled = this.intervalCheckBox.Checked;
            this.fillValueNumericUpDown.Enabled = this.intervalCheckBox.Checked && this.fillMethodCombox.SelectedIndex == 2;
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.aggListView = new System.Windows.Forms.ListView();
            this.tableLayoutPanel11 = new System.Windows.Forms.TableLayoutPanel();
            this.unselectButton = new System.Windows.Forms.Button();
            this.label12 = new System.Windows.Forms.Label();
            this.rawListView = new System.Windows.Forms.ListView();
            this.tableLayoutPanel5 = new System.Windows.Forms.TableLayoutPanel();
            this.groupbyCheckBox = new System.Windows.Forms.CheckBox();
            this.groupbyComboBox = new System.Windows.Forms.ComboBox();
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
            this.toTimePicker = new System.Windows.Forms.DateTimePicker();
            this.fromTimePicker = new System.Windows.Forms.DateTimePicker();
            this.tableLayoutPanel6 = new System.Windows.Forms.TableLayoutPanel();
            this.intervalTimeUnitComboBox = new System.Windows.Forms.ComboBox();
            this.intervalTimeNumericUpDown = new System.Windows.Forms.NumericUpDown();
            this.intervalCheckBox = new System.Windows.Forms.CheckBox();
            this.tableLayoutPanel7 = new System.Windows.Forms.TableLayoutPanel();
            this.fillMethodCombox = new System.Windows.Forms.ComboBox();
            this.fillValueNumericUpDown = new System.Windows.Forms.NumericUpDown();
            this.tableLayoutPanel9 = new System.Windows.Forms.TableLayoutPanel();
            this.inputButton = new System.Windows.Forms.Button();
            this.label6 = new System.Windows.Forms.Label();
            this.inputTextBox = new System.Windows.Forms.RichTextBox();
            this.tableLayoutPanel4 = new System.Windows.Forms.TableLayoutPanel();
            this.showButton = new System.Windows.Forms.Button();
            this.label7 = new System.Windows.Forms.Label();
            this.tableLayoutPanel10 = new System.Windows.Forms.TableLayoutPanel();
            this.functionCombox = new System.Windows.Forms.ComboBox();
            this.label11 = new System.Windows.Forms.Label();
            this.tableLayoutPanel1.SuspendLayout();
            this.tableLayoutPanel11.SuspendLayout();
            this.tableLayoutPanel5.SuspendLayout();
            this.tableLayoutPanel2.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.tableLayoutPanel6.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.intervalTimeNumericUpDown)).BeginInit();
            this.tableLayoutPanel7.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).BeginInit();
            this.tableLayoutPanel9.SuspendLayout();
            this.tableLayoutPanel4.SuspendLayout();
            this.tableLayoutPanel10.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.aggListView, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel11, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.rawListView, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel5, 0, 15);
            this.tableLayoutPanel1.Controls.Add(this.headsCheck, 0, 13);
            this.tableLayoutPanel1.Controls.Add(this.timestampCheck, 0, 14);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 19);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel2, 0, 20);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 18);
            this.tableLayoutPanel1.Controls.Add(this.label3, 0, 12);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 21);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 22);
            this.tableLayoutPanel1.Controls.Add(this.label5, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.label8, 0, 8);
            this.tableLayoutPanel1.Controls.Add(this.label9, 0, 10);
            this.tableLayoutPanel1.Controls.Add(this.toTimePicker, 0, 11);
            this.tableLayoutPanel1.Controls.Add(this.fromTimePicker, 0, 9);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel6, 0, 16);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel7, 0, 17);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel9, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.inputTextBox, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel4, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel10, 0, 5);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 24;
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
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 1200);
            this.tableLayoutPanel1.TabIndex = 0;
            // 
            // aggListView
            // 
            this.aggListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.aggListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.aggListView.Location = new System.Drawing.Point(18, 422);
            this.aggListView.Name = "aggListView";
            this.aggListView.Size = new System.Drawing.Size(464, 120);
            this.aggListView.TabIndex = 3;
            this.aggListView.UseCompatibleStateImageBehavior = false;
            this.aggListView.View = System.Windows.Forms.View.Details;
            // 
            // tableLayoutPanel11
            // 
            this.tableLayoutPanel11.ColumnCount = 2;
            this.tableLayoutPanel11.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel11.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel11.Controls.Add(this.unselectButton, 0, 0);
            this.tableLayoutPanel11.Controls.Add(this.label12, 0, 0);
            this.tableLayoutPanel11.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel11.Location = new System.Drawing.Point(18, 381);
            this.tableLayoutPanel11.Name = "tableLayoutPanel11";
            this.tableLayoutPanel11.RowCount = 1;
            this.tableLayoutPanel11.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel11.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel11.TabIndex = 34;
            // 
            // unselectButton
            // 
            this.unselectButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.unselectButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.unselectButton.Location = new System.Drawing.Point(351, 3);
            this.unselectButton.Name = "unselectButton";
            this.unselectButton.Size = new System.Drawing.Size(110, 29);
            this.unselectButton.TabIndex = 3;
            this.unselectButton.Text = "Remove";
            this.unselectButton.UseVisualStyleBackColor = true;
            this.unselectButton.Click += new System.EventHandler(this.UnselectButton_Click);
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label12.Location = new System.Drawing.Point(0, 6);
            this.label12.Margin = new System.Windows.Forms.Padding(0, 6, 3, 3);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(345, 26);
            this.label12.TabIndex = 19;
            this.label12.Text = "aggregations";
            // 
            // rawListView
            // 
            this.rawListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rawListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawListView.Location = new System.Drawing.Point(18, 214);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(464, 120);
            this.rawListView.TabIndex = 2;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // tableLayoutPanel5
            // 
            this.tableLayoutPanel5.ColumnCount = 2;
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 35F));
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 65F));
            this.tableLayoutPanel5.Controls.Add(this.groupbyCheckBox, 0, 0);
            this.tableLayoutPanel5.Controls.Add(this.groupbyComboBox, 1, 0);
            this.tableLayoutPanel5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel5.Location = new System.Drawing.Point(18, 766);
            this.tableLayoutPanel5.Name = "tableLayoutPanel5";
            this.tableLayoutPanel5.RowCount = 1;
            this.tableLayoutPanel5.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel5.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel5.TabIndex = 23;
            // 
            // groupbyCheckBox
            // 
            this.groupbyCheckBox.AutoSize = true;
            this.groupbyCheckBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.groupbyCheckBox.Location = new System.Drawing.Point(0, 3);
            this.groupbyCheckBox.Margin = new System.Windows.Forms.Padding(0, 3, 3, 3);
            this.groupbyCheckBox.Name = "groupbyCheckBox";
            this.groupbyCheckBox.Size = new System.Drawing.Size(159, 29);
            this.groupbyCheckBox.TabIndex = 2;
            this.groupbyCheckBox.Text = "group by";
            this.groupbyCheckBox.UseVisualStyleBackColor = true;
            this.groupbyCheckBox.CheckedChanged += new System.EventHandler(this.GroupbyCheckBox_CheckedChanged);
            // 
            // groupbyComboBox
            // 
            this.groupbyComboBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.groupbyComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.groupbyComboBox.FormattingEnabled = true;
            this.groupbyComboBox.Location = new System.Drawing.Point(165, 3);
            this.groupbyComboBox.Name = "groupbyComboBox";
            this.groupbyComboBox.Size = new System.Drawing.Size(296, 30);
            this.groupbyComboBox.TabIndex = 0;
            // 
            // headsCheck
            // 
            this.headsCheck.AutoSize = true;
            this.headsCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.headsCheck.Location = new System.Drawing.Point(18, 702);
            this.headsCheck.Name = "headsCheck";
            this.headsCheck.Size = new System.Drawing.Size(464, 26);
            this.headsCheck.TabIndex = 6;
            this.headsCheck.Text = "show heads";
            this.headsCheck.UseVisualStyleBackColor = true;
            // 
            // timestampCheck
            // 
            this.timestampCheck.AutoSize = true;
            this.timestampCheck.Dock = System.Windows.Forms.DockStyle.Fill;
            this.timestampCheck.Location = new System.Drawing.Point(18, 734);
            this.timestampCheck.Name = "timestampCheck";
            this.timestampCheck.Size = new System.Drawing.Size(464, 26);
            this.timestampCheck.TabIndex = 7;
            this.timestampCheck.Text = "display as timestamp";
            this.timestampCheck.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 917);
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
            this.tableLayoutPanel2.Location = new System.Drawing.Point(18, 945);
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
            this.outputTextbox.TabIndex = 0;
            // 
            // outputButton
            // 
            this.outputButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.outputButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.outputButton.Location = new System.Drawing.Point(351, 3);
            this.outputButton.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.outputButton.Name = "outputButton";
            this.outputButton.Size = new System.Drawing.Size(113, 29);
            this.outputButton.TabIndex = 1;
            this.outputButton.Text = "Select";
            this.outputButton.UseVisualStyleBackColor = true;
            this.outputButton.Click += new System.EventHandler(this.OutputButton_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Location = new System.Drawing.Point(18, 889);
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
            this.label3.Location = new System.Drawing.Point(18, 674);
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
            this.label4.Location = new System.Drawing.Point(18, 986);
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
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 1014);
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
            this.label5.TabIndex = 0;
            this.label5.Text = "Input";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label8.Location = new System.Drawing.Point(18, 548);
            this.label8.Margin = new System.Windows.Forms.Padding(3);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(464, 22);
            this.label8.TabIndex = 18;
            this.label8.Text = "timestamp from";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(18, 611);
            this.label9.Margin = new System.Windows.Forms.Padding(3);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(130, 22);
            this.label9.TabIndex = 19;
            this.label9.Text = "timestamp to";
            // 
            // toTimePicker
            // 
            this.toTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.toTimePicker.Dock = System.Windows.Forms.DockStyle.Fill;
            this.toTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.toTimePicker.Location = new System.Drawing.Point(18, 639);
            this.toTimePicker.Name = "toTimePicker";
            this.toTimePicker.Size = new System.Drawing.Size(464, 29);
            this.toTimePicker.TabIndex = 5;
            // 
            // fromTimePicker
            // 
            this.fromTimePicker.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            this.fromTimePicker.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fromTimePicker.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.fromTimePicker.Location = new System.Drawing.Point(18, 576);
            this.fromTimePicker.Name = "fromTimePicker";
            this.fromTimePicker.Size = new System.Drawing.Size(464, 29);
            this.fromTimePicker.TabIndex = 4;
            // 
            // tableLayoutPanel6
            // 
            this.tableLayoutPanel6.ColumnCount = 3;
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 35F));
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 40F));
            this.tableLayoutPanel6.Controls.Add(this.intervalTimeUnitComboBox, 0, 0);
            this.tableLayoutPanel6.Controls.Add(this.intervalTimeNumericUpDown, 0, 0);
            this.tableLayoutPanel6.Controls.Add(this.intervalCheckBox, 0, 0);
            this.tableLayoutPanel6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel6.Location = new System.Drawing.Point(18, 807);
            this.tableLayoutPanel6.Name = "tableLayoutPanel6";
            this.tableLayoutPanel6.RowCount = 1;
            this.tableLayoutPanel6.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel6.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel6.TabIndex = 24;
            // 
            // intervalTimeUnitComboBox
            // 
            this.intervalTimeUnitComboBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.intervalTimeUnitComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.intervalTimeUnitComboBox.FormattingEnabled = true;
            this.intervalTimeUnitComboBox.Location = new System.Drawing.Point(281, 3);
            this.intervalTimeUnitComboBox.Name = "intervalTimeUnitComboBox";
            this.intervalTimeUnitComboBox.Size = new System.Drawing.Size(180, 30);
            this.intervalTimeUnitComboBox.TabIndex = 1;
            // 
            // intervalTimeNumericUpDown
            // 
            this.intervalTimeNumericUpDown.Dock = System.Windows.Forms.DockStyle.Fill;
            this.intervalTimeNumericUpDown.Location = new System.Drawing.Point(165, 3);
            this.intervalTimeNumericUpDown.Name = "intervalTimeNumericUpDown";
            this.intervalTimeNumericUpDown.Size = new System.Drawing.Size(110, 29);
            this.intervalTimeNumericUpDown.TabIndex = 0;
            // 
            // intervalCheckBox
            // 
            this.intervalCheckBox.AutoSize = true;
            this.intervalCheckBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.intervalCheckBox.Location = new System.Drawing.Point(0, 3);
            this.intervalCheckBox.Margin = new System.Windows.Forms.Padding(0, 3, 3, 3);
            this.intervalCheckBox.Name = "intervalCheckBox";
            this.intervalCheckBox.Size = new System.Drawing.Size(159, 29);
            this.intervalCheckBox.TabIndex = 0;
            this.intervalCheckBox.Text = "interval";
            this.intervalCheckBox.UseVisualStyleBackColor = true;
            this.intervalCheckBox.CheckedChanged += new System.EventHandler(this.IntervalCheckBox_CheckedChanged);
            // 
            // tableLayoutPanel7
            // 
            this.tableLayoutPanel7.ColumnCount = 3;
            this.tableLayoutPanel7.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 35F));
            this.tableLayoutPanel7.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 40F));
            this.tableLayoutPanel7.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel7.Controls.Add(this.fillMethodCombox, 1, 0);
            this.tableLayoutPanel7.Controls.Add(this.fillValueNumericUpDown, 2, 0);
            this.tableLayoutPanel7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel7.Location = new System.Drawing.Point(18, 848);
            this.tableLayoutPanel7.Name = "tableLayoutPanel7";
            this.tableLayoutPanel7.RowCount = 1;
            this.tableLayoutPanel7.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel7.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel7.TabIndex = 26;
            // 
            // fillMethodCombox
            // 
            this.fillMethodCombox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fillMethodCombox.FormattingEnabled = true;
            this.fillMethodCombox.Location = new System.Drawing.Point(162, 3);
            this.fillMethodCombox.Margin = new System.Windows.Forms.Padding(0, 3, 3, 3);
            this.fillMethodCombox.Name = "fillMethodCombox";
            this.fillMethodCombox.Size = new System.Drawing.Size(182, 30);
            this.fillMethodCombox.TabIndex = 0;
            this.fillMethodCombox.SelectedIndexChanged += new System.EventHandler(this.FillMethodCombox_SelectedIndexChanged);
            // 
            // fillValueNumericUpDown
            // 
            this.fillValueNumericUpDown.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fillValueNumericUpDown.Location = new System.Drawing.Point(350, 3);
            this.fillValueNumericUpDown.Name = "fillValueNumericUpDown";
            this.fillValueNumericUpDown.Size = new System.Drawing.Size(111, 29);
            this.fillValueNumericUpDown.TabIndex = 1;
            // 
            // tableLayoutPanel9
            // 
            this.tableLayoutPanel9.ColumnCount = 2;
            this.tableLayoutPanel9.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel9.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel9.Controls.Add(this.inputButton, 0, 0);
            this.tableLayoutPanel9.Controls.Add(this.label6, 0, 0);
            this.tableLayoutPanel9.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel9.Location = new System.Drawing.Point(18, 46);
            this.tableLayoutPanel9.Name = "tableLayoutPanel9";
            this.tableLayoutPanel9.RowCount = 1;
            this.tableLayoutPanel9.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel9.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel9.TabIndex = 2;
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
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label6.Location = new System.Drawing.Point(0, 6);
            this.label6.Margin = new System.Windows.Forms.Padding(0, 6, 3, 3);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(345, 26);
            this.label6.TabIndex = 0;
            this.label6.Text = "name or tables or stable";
            // 
            // inputTextBox
            // 
            this.inputTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputTextBox.Location = new System.Drawing.Point(18, 87);
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(464, 80);
            this.inputTextBox.TabIndex = 1;
            this.inputTextBox.Text = "";
            // 
            // tableLayoutPanel4
            // 
            this.tableLayoutPanel4.ColumnCount = 2;
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel4.Controls.Add(this.showButton, 0, 0);
            this.tableLayoutPanel4.Controls.Add(this.label7, 0, 0);
            this.tableLayoutPanel4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel4.Location = new System.Drawing.Point(18, 173);
            this.tableLayoutPanel4.Name = "tableLayoutPanel4";
            this.tableLayoutPanel4.RowCount = 1;
            this.tableLayoutPanel4.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel4.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel4.TabIndex = 31;
            // 
            // showButton
            // 
            this.showButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.showButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.showButton.Location = new System.Drawing.Point(351, 3);
            this.showButton.Name = "showButton";
            this.showButton.Size = new System.Drawing.Size(110, 29);
            this.showButton.TabIndex = 1;
            this.showButton.Text = "Show";
            this.showButton.UseVisualStyleBackColor = true;
            this.showButton.Click += new System.EventHandler(this.ShowButton_Click);
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label7.Location = new System.Drawing.Point(0, 6);
            this.label7.Margin = new System.Windows.Forms.Padding(0, 6, 3, 3);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(345, 26);
            this.label7.TabIndex = 0;
            this.label7.Text = "columns";
            // 
            // tableLayoutPanel10
            // 
            this.tableLayoutPanel10.ColumnCount = 2;
            this.tableLayoutPanel10.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel10.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel10.Controls.Add(this.functionCombox, 0, 0);
            this.tableLayoutPanel10.Controls.Add(this.label11, 0, 0);
            this.tableLayoutPanel10.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel10.Location = new System.Drawing.Point(18, 340);
            this.tableLayoutPanel10.Name = "tableLayoutPanel10";
            this.tableLayoutPanel10.RowCount = 1;
            this.tableLayoutPanel10.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel10.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel10.TabIndex = 33;
            // 
            // functionCombox
            // 
            this.functionCombox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.functionCombox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.functionCombox.FormattingEnabled = true;
            this.functionCombox.Location = new System.Drawing.Point(351, 3);
            this.functionCombox.MaxDropDownItems = 11;
            this.functionCombox.Name = "functionCombox";
            this.functionCombox.Size = new System.Drawing.Size(110, 30);
            this.functionCombox.TabIndex = 2;
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label11.Location = new System.Drawing.Point(0, 6);
            this.label11.Margin = new System.Windows.Forms.Padding(0, 6, 3, 3);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(345, 26);
            this.label11.TabIndex = 21;
            this.label11.Text = "function";
            // 
            // TDAggregationForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 1200);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDAggregationForm";
            this.Size = new System.Drawing.Size(500, 1200);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.tableLayoutPanel11.ResumeLayout(false);
            this.tableLayoutPanel11.PerformLayout();
            this.tableLayoutPanel5.ResumeLayout(false);
            this.tableLayoutPanel5.PerformLayout();
            this.tableLayoutPanel2.ResumeLayout(false);
            this.tableLayoutPanel2.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.tableLayoutPanel6.ResumeLayout(false);
            this.tableLayoutPanel6.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.intervalTimeNumericUpDown)).EndInit();
            this.tableLayoutPanel7.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.fillValueNumericUpDown)).EndInit();
            this.tableLayoutPanel9.ResumeLayout(false);
            this.tableLayoutPanel9.PerformLayout();
            this.tableLayoutPanel4.ResumeLayout(false);
            this.tableLayoutPanel4.PerformLayout();
            this.tableLayoutPanel10.ResumeLayout(false);
            this.tableLayoutPanel10.PerformLayout();
            this.ResumeLayout(false);

        }
    }

    internal class TDAggParameter
    {
        public int threadIndex;
        public TDTable table;
        public String[] selectedFields;
        public String from;
        public String to;
        public bool groupByChecked;
        public String groupByName;
        public bool intervalCheck;
        public int intervalTime;
        public String intervalType;
        public String intervalMethod;
        public double intervelMethodValue;
        public TDHttpTimestampType displayAsTimestamp;
        public ArrayList results;
    }

}

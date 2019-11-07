using System;
using System.Linq;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;

namespace excel2010
{
    public partial class TDAggregationForm : TDForm
    {
        private bool isSelectOutputing = true;
       
        public TDAggregationForm()
        {
            InitializeComponent();

            foreach (String aggFunction in Globals.ThisAddIn.tdUtil.aggFunctions)
            {
                this.functionCombox.Items.Add(aggFunction);
            }
            this.functionCombox.SelectedIndex = Globals.ThisAddIn.tdUtil.aggFunctions.Length - 1;

            foreach (String intervalTimeUnit in Globals.ThisAddIn.tdUtil.intervalTimeUnits)
            {
                this.intervalTimeUnitComboBox.Items.Add(intervalTimeUnit);
            }

            foreach (String fillMethod in Globals.ThisAddIn.tdUtil.fillMethods)
            {
                this.fillMethodCombox.Items.Add(fillMethod);
            }
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = Globals.ThisAddIn.tdPersist.aggInput;
            this.FillListView();

            if (Globals.ThisAddIn.tdPersist.aggFromTimestamp == 0 && Globals.ThisAddIn.tdPersist.aggToTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                DateTime end = (DateTime.Now.AddDays(1)).Date;
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }
            else
            {
                DateTime begin = new DateTime(Globals.ThisAddIn.tdPersist.aggFromTimestamp);
                DateTime end = new DateTime(Globals.ThisAddIn.tdPersist.aggToTimestamp);
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }

            this.headsCheck.Checked = Globals.ThisAddIn.tdPersist.aggShowHeads;
            this.timestampCheck.Checked = Globals.ThisAddIn.tdPersist.aggDisplayAsTimestamp;

            this.groupbyCheckBox.Checked = Globals.ThisAddIn.tdPersist.aggGroupByCheck;
            this.intervalCheckBox.Checked = Globals.ThisAddIn.tdPersist.aggIntervalCheck;

            this.UpdateControls();

            if (this.groupbyComboBox.Items.Count != 0)
            {
                this.groupbyComboBox.SelectedIndex = Globals.ThisAddIn.tdPersist.aggGroupbyIndex;
            }

            this.intervalTimeNumericUpDown.Value = Globals.ThisAddIn.tdPersist.aggIntervalTime;
            this.intervalTimeUnitComboBox.SelectedIndex = Globals.ThisAddIn.tdPersist.aggIntervalTimeUnitIndex;
            this.fillMethodCombox.SelectedIndex = Globals.ThisAddIn.tdPersist.aggFillMethodIndex;
            this.fillValueNumericUpDown.Value = (int)Globals.ThisAddIn.tdPersist.aggFillMethodValue;
            this.fillValueNumericUpDown.DecimalPlaces = 2;

            this.SelectListView();
            this.outputTextbox.Text = Globals.ThisAddIn.tdPersist.aggOutput;
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.aggInput = this.inputTextBox.Text;
            Globals.ThisAddIn.tdPersist.aggFromTimestamp = fromTimePicker.Value.Ticks;
            Globals.ThisAddIn.tdPersist.aggToTimestamp = toTimePicker.Value.Ticks;
            Globals.ThisAddIn.tdPersist.aggShowHeads = this.headsCheck.Checked;
            Globals.ThisAddIn.tdPersist.aggDisplayAsTimestamp = this.timestampCheck.Checked;

            Globals.ThisAddIn.tdPersist.aggGroupByCheck = this.groupbyCheckBox.Checked;
            if (this.groupbyComboBox.Items.Count != 0)
            {
                Globals.ThisAddIn.tdPersist.aggGroupbyIndex = this.groupbyComboBox.SelectedIndex;
            }

            Globals.ThisAddIn.tdPersist.aggIntervalCheck = this.intervalCheckBox.Checked;
            Globals.ThisAddIn.tdPersist.aggIntervalTime = Decimal.ToInt32(this.intervalTimeNumericUpDown.Value);
            Globals.ThisAddIn.tdPersist.aggIntervalTimeUnitIndex = this.intervalTimeUnitComboBox.SelectedIndex;
            Globals.ThisAddIn.tdPersist.aggFillMethodIndex = this.fillMethodCombox.SelectedIndex;
            Globals.ThisAddIn.tdPersist.aggFillMethodValue = Decimal.ToDouble(this.fillValueNumericUpDown.Value);

            Globals.ThisAddIn.tdPersist.aggSelectFields.Clear();
            foreach (ListViewItem lvi in this.selectListView.CheckedItems)
            {
                Globals.ThisAddIn.tdPersist.aggSelectFields.Add(lvi.Text);
            }

            Globals.ThisAddIn.tdPersist.aggOutput = this.outputTextbox.Text;
        }

        private void Form_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == (char)Keys.Escape)
            {
                this.GetFactory().CloseForm();
            }
        }

        public override void SheetSelectionChange(Worksheet sheet, Range Target)
        {
            this.GetFactory().StopSelect();

            if (Target != null)
            {
                if (this.isSelectOutputing)
                {
                    String selectRange = Target.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
                    this.outputTextbox.Text = Globals.ThisAddIn.tdUtil.SelectRangeByString(selectRange, TDFormSelectType.TD_FORM_SELECT_CELL);
                }
                else
                {
                    this.inputTextBox.Text = Globals.ThisAddIn.tdUtil.GetRangeValues(Target);
                    this.FillListView();
                }
            }
        }
        
        private void InputTextbox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == System.Convert.ToChar(13))
            {
                this.FillListView();
            }
        }

        private void InputTextbox_MouseClick(object sender, MouseEventArgs e)
        {
            this.GetFactory().StopSelect();
        }

        private void InputButton_Click(object sender, EventArgs e)
        {
            this.isSelectOutputing = false;
            this.GetFactory().StartSelect();
        }
        
        private void GroupbyCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
        }

        private void IntervalCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
        }

        private void OutputTextbox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == System.Convert.ToChar(13))
            {
                Globals.ThisAddIn.tdUtil.SelectRangeByString(outputTextbox.Text, TDFormSelectType.TD_FORM_SELECT_CELL);
                e.Handled = true;
            }
        }

        private void FillMethodCombox_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.UpdateControls();
        }

        private void SelectCombox_SelectedIndexChanged(object sender, EventArgs e)
        {
            String[] selectFields = this.GetRawListViewCheckedItems();
            if (selectFields.Length == 0)
            {
                this.functionCombox.SelectedIndex = Globals.ThisAddIn.tdUtil.aggFunctions.Length - 1;
                return;
            }

            int aggFuncIndex = this.functionCombox.SelectedIndex;
            if (aggFuncIndex >= Globals.ThisAddIn.tdUtil.aggFunctions.Length - 1)
            {
                this.functionCombox.SelectedIndex = Globals.ThisAddIn.tdUtil.aggFunctions.Length - 1;
                return;
            }

            String aggFuncName = Globals.ThisAddIn.tdUtil.aggFunctions[aggFuncIndex];
            foreach (String field in selectFields)
            {
                ListViewItem lvi = new ListViewItem();
                //the first column such as ts, can be used by count function only
                if (this.rawListView.Items[0].Text == field && aggFuncName != "count")
                {
                    continue;
                }

                lvi.Text = aggFuncName + "(" + field + ")"; ;
                this.selectListView.Items.Add(lvi);
            }

            this.functionCombox.SelectedIndex = Globals.ThisAddIn.tdUtil.aggFunctions.Length - 1;
        }

        private void UnselectButton_Click(object sender, EventArgs e)
        {
            this.DeleteSelectListSelectedItems();
        }

        private void OutputTextbox_MouseClick(object sender, MouseEventArgs e)
        {
            this.GetFactory().StopSelect();
        }

        private void OutputButton_Click(object sender, EventArgs e)
        {
            this.isSelectOutputing = true;
            this.GetFactory().StartSelect();
        }

        private void Import_Click(object sender, EventArgs e)
        {
            this.GetFactory().StopSelect();
            
            String tableName = this.inputTextBox.Text;
            if (tableName == "")
            {
                Globals.ThisAddIn.tdUtil.ShowError("table name not input");
                return;
            }

            String[] selectedFields = this.GetSelectListViewItems();
            if (selectedFields.Length <= 0)
            {
                Globals.ThisAddIn.tdUtil.ShowError("no fields select");
                return;
            }

            if (selectedFields.Length > 250)
            {
                Globals.ThisAddIn.tdUtil.ShowError("too many select fields");
                return;
            }

            String mtName = "";
            int tablenameDotIndex = tableName.IndexOf(',');
            if (tablenameDotIndex != -1)
            {
                String tablename = tableName.Substring(0, tablenameDotIndex);
                if (!Globals.ThisAddIn.tdUtil.IsTablesName(tablename))
                {
                    Globals.ThisAddIn.tdUtil.ShowError(tablename + " is not from any stable");
                    return;
                }

                mtName = Globals.ThisAddIn.tdUtil.GetMetricsNameOfTable(tablename);
                if (mtName == "")
                {
                    Globals.ThisAddIn.tdUtil.ShowError(tablename + " should not be a stable");
                    return;
                }
            }
            else
            {
                mtName = Globals.ThisAddIn.tdUtil.GetMetricsNameOfTable(tableName);
            }

            String from = fromTimePicker.Text;
            String to = toTimePicker.Text;

            bool groupByChecked = this.groupbyCheckBox.Checked;
            String groupByName = this.groupbyComboBox.Text;

            bool intervalCheck = this.intervalCheckBox.Checked;
            int intervalTime = Decimal.ToInt32(this.intervalTimeNumericUpDown.Value);
            String intervalType = Globals.ThisAddIn.tdUtil.intervalTimeUnitTypes[this.intervalTimeUnitComboBox.SelectedIndex];
            String intervalMethod = Globals.ThisAddIn.tdUtil.fillMethods[this.fillMethodCombox.SelectedIndex];
            double intervelMethodValue = Decimal.ToDouble(this.fillValueNumericUpDown.Value);

            String sql = this.GenerateSql(mtName, tableName, selectedFields, from, to
                , groupByChecked, groupByName
                , intervalCheck, intervalTime, intervalType, intervalMethod, intervelMethodValue);

            Range outputRange = Globals.ThisAddIn.tdUtil.GetRange(outputTextbox.Text);
            if (outputRange == null)
            {
                Globals.ThisAddIn.tdUtil.ShowError("output columns not select");
                return;
            }

            bool displayAsTimestamp = this.timestampCheck.Checked;
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, displayAsTimestamp);
            if (jo != null)
            {
                Globals.ThisAddIn.Application.ScreenUpdating = false;
                this.FillExcel(jo, outputRange);
                Globals.ThisAddIn.Application.ScreenUpdating = true;
            }
        }

        private void ClearGroupByCombox()
        {
            this.groupbyComboBox.Items.Clear();
            //this.groupbyCheckBox.Checked = false;
        }

        private void FillListView()
        {
            this.ClearListView();
            this.ClearGroupByCombox();
            
            String tablename = this.inputTextBox.Text;
            if (tablename == "")
            {
                //Globals.ThisAddIn.tdUtil.ShowError("table name not input");
                return;
            }

            bool isFromMetrics = false;
            int tablenameDotIndex = tablename.IndexOf(',');
            if (tablenameDotIndex != -1)
            {
                tablename = tablename.Substring(0, tablenameDotIndex);
                if (!Globals.ThisAddIn.tdUtil.IsTablesName(tablename))
                {
                    Globals.ThisAddIn.tdUtil.ShowError(tablename + " is not from any stable");
                    return;
                }

                if (Globals.ThisAddIn.tdUtil.GetMetricsNameOfTable(tablename) == "")
                {
                    Globals.ThisAddIn.tdUtil.ShowError(tablename + " should not be a stable");
                    return;
                }

                isFromMetrics = true;
            }
            else
            {
                isFromMetrics = Globals.ThisAddIn.tdUtil.IsMetricsName(tablename);
                if (!isFromMetrics)
                {
                    isFromMetrics = (Globals.ThisAddIn.tdUtil.GetMetricsNameOfTable(tablename) != "");
                }
            }

            String sql = "describe " + Globals.ThisAddIn.tdPersist.DB + "." + tablename;
            JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
            
            if (jo != null)
            {
                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                if (headLength != 4) {
                    Globals.ThisAddIn.tdUtil.ShowError("invalid response from server");
                    return;
                }
                
                if (dataLength < 2)
                {
                    Globals.ThisAddIn.tdUtil.ShowError("invalid table");
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
                    if (tag != "")
                    {
                        if (isFromMetrics)
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
            }

            if (this.groupbyComboBox.Items.Count != 0)
            {
                this.groupbyComboBox.SelectedIndex = Math.Max(this.groupbyComboBox.SelectedIndex, this.groupbyComboBox.Items.Count - 1);
                this.groupbyComboBox.SelectedIndex = Math.Min(this.groupbyComboBox.SelectedIndex, 0);
            }
        }
        
        private void ClearListView()
        {
            this.rawListView.BeginUpdate();
            this.rawListView.Clear();
            
            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = "Field Name";
            ch1.Width = 230;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);
            
            ColumnHeader ch2 = new ColumnHeader();
            ch2.Text = "Field Type";
            ch2.Width = 110;
            ch2.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch2);

            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.rawListView.CheckBoxes = true;

            this.rawListView.EndUpdate();

            this.selectListView.BeginUpdate();
            this.selectListView.Clear();

            ColumnHeader ch3 = new ColumnHeader();
            ch3.Text = "Selected Field Name";
            ch3.Width = 188;
            ch3.TextAlign = HorizontalAlignment.Left;
            this.selectListView.Columns.Add(ch3);

            this.selectListView.GridLines = true;
            this.selectListView.FullRowSelect = true;
            this.selectListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.selectListView.CheckBoxes = true;

            this.selectListView.EndUpdate();
        }
        
        private void SelectListView()
        {
            foreach (ListViewItem lvi in this.selectListView.Items)
            {
                foreach (String field in Globals.ThisAddIn.tdPersist.aggSelectFields)
                {
                    if (field == lvi.Text)
                    {
                        lvi.Checked = true;
                    }
                }
            }
        }

        private String[] GetRawListViewCheckedItems()
        {
            String[] selectItems;

            if (this.rawListView.CheckedItems.Count == 0)
            {
                selectItems = new String[0];
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

        private String[] GetSelectListViewItems()
        {
            String[] items;

            if (this.selectListView.Items.Count == 0)
            {
                items = new String[0];
            }
            else
            {
                items = new String[this.selectListView.Items.Count];
                int i = 0;
                foreach (ListViewItem lvi in this.selectListView.Items)
                {
                    items[i++] = lvi.Text;
                }
            }

            return items;
        }

        private void DeleteSelectListSelectedItems()
        {
            foreach (ListViewItem lvi in this.selectListView.CheckedItems)
            {
                this.selectListView.Items.Remove(lvi);
            }
        }

        private void FillExcel(JObject jo, Range range)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;

                Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;
                range.UnMerge();
                int beginRow = range.Row;
                int beginCol = range.Column;

                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                int beginOffset = 0;
                int[] rowArray = Globals.ThisAddIn.tdUtil.GetUnHiddenRows(beginRow, showHeads ? dataLength + 1 : dataLength);
                int[] colArray = Globals.ThisAddIn.tdUtil.GetUnHiddenColumns(beginCol, headLength);
  
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
                    Globals.ThisAddIn.tdUtil.ShowError("no data was found.");
                    return;
                }
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }
        }

        private String GenerateSql(String mt, String table, String[] fields, String from, String to
             , bool groupByChecked, String groupByName
             , bool intervalCheck, Int32 intervalTime, String intervalType, String intervalMethod, Double intervelMethodValue)
        {
            String sql = "select " + fields[0];
            for (int i = 1; i < fields.Length; ++i)
            {
                sql = sql + ", " + fields[i];
            }

            if (mt == "")
            {
                sql = sql + " from " + Globals.ThisAddIn.tdPersist.DB + "." + table + " where _c0 >= '" + from + "' and _c0 < '" + to + "'";
            }
            else
            {
                String tableNames = table.Replace(",", "','");
                sql = sql + " from " + Globals.ThisAddIn.tdPersist.DB + "." + mt + " where tbname in('" + tableNames + "') and _c0>='" + from + "' and _c0<'" + to + "'";
            }

            if (intervalCheck)
            {
                sql = sql + " interval(" + intervalTime.ToString() + intervalType + ")";
                if (intervalMethod != "value")
                {
                    sql = sql + " fill(" + intervalMethod + ")";
                }
                else
                {
                    sql = sql + " fill(" + intervalMethod + ", " + intervelMethodValue.ToString() + ")";
                }
            }

            if (groupByChecked && groupByName != "")
            {
                sql = sql + " group by " + groupByName;
            }

            return sql;
        }

        private void UpdateControls()
        {
            this.groupbyComboBox.Enabled = this.groupbyCheckBox.Checked;
            this.intervalTimeNumericUpDown.Enabled = this.intervalCheckBox.Checked;
            this.intervalTimeUnitComboBox.Enabled = this.intervalCheckBox.Checked;
            this.fillMethodCombox.Enabled = this.intervalCheckBox.Checked;
            this.fillValueNumericUpDown.Enabled = this.intervalCheckBox.Checked && this.fillMethodCombox.SelectedIndex == 2;
        }
    }
}

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
    public partial class TDDetailsForm : TDForm
    {
        private bool isSelectOutputing = true;
        
        public TDDetailsForm()
        {
            InitializeComponent();
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = Globals.ThisAddIn.tdPersist.detailInput;
            this.FillListView();

            if (Globals.ThisAddIn.tdPersist.detailFromTimestamp == 0 && Globals.ThisAddIn.tdPersist.detailToTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                DateTime end = (DateTime.Now.AddDays(1)).Date;
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }
            else
            {
                DateTime begin = new DateTime(Globals.ThisAddIn.tdPersist.detailFromTimestamp);
                DateTime end = new DateTime(Globals.ThisAddIn.tdPersist.detailToTimestamp);
                fromTimePicker.Value = begin;
                toTimePicker.Value = end;
            }

            this.headsCheck.Checked = Globals.ThisAddIn.tdPersist.detailShowHeads;
            this.timestampCheck.Checked = Globals.ThisAddIn.tdPersist.detailDisplayAsTimestamp;
            this.ascCheck.Checked = Globals.ThisAddIn.tdPersist.detailAscend;
            this.limitrowsNumericUpDown.Value = Globals.ThisAddIn.tdPersist.detailLimitRows;

            this.SelectListView();
            this.outputTextbox.Text = Globals.ThisAddIn.tdPersist.detailOutput;
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.detailInput = this.inputTextBox.Text;
            Globals.ThisAddIn.tdPersist.detailFromTimestamp = fromTimePicker.Value.Ticks;
            Globals.ThisAddIn.tdPersist.detailToTimestamp = toTimePicker.Value.Ticks;

            Globals.ThisAddIn.tdPersist.detailShowHeads = this.headsCheck.Checked;
            Globals.ThisAddIn.tdPersist.detailDisplayAsTimestamp = this.timestampCheck.Checked;
            Globals.ThisAddIn.tdPersist.detailAscend = this.ascCheck.Checked;
            Globals.ThisAddIn.tdPersist.detailLimitRows = Decimal.ToInt32(this.limitrowsNumericUpDown.Value);

            Globals.ThisAddIn.tdPersist.detailSelectFields.Clear();
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                Globals.ThisAddIn.tdPersist.detailSelectFields.Add(lvi.Text);
            }

            Globals.ThisAddIn.tdPersist.detailOutput = this.outputTextbox.Text;
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
                    this.inputTextBox.Text = Globals.ThisAddIn.tdUtil.GetRangeFirstValue(Target);
                    String selectRange = Target.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
                    Globals.ThisAddIn.tdUtil.SelectRangeByString(selectRange, TDFormSelectType.TD_FORM_SELECT_CELL);
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

        private void OutputTextbox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == System.Convert.ToChar(13))
            {
                Globals.ThisAddIn.tdUtil.SelectRangeByString(outputTextbox.Text, TDFormSelectType.TD_FORM_SELECT_CELL);
                e.Handled = true;
            }
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

            String[] selectedFields = this.GetListViewCheckedItems();
            String from = fromTimePicker.Text;
            String to = toTimePicker.Text;
            bool asc = ascCheck.Checked;
            int limitRows = Decimal.ToInt32(this.limitrowsNumericUpDown.Value);
            if (asc)
            {
                if (Globals.ThisAddIn.tdUtil.IsMetricsName(tableName))
                {
                    Globals.ThisAddIn.tdUtil.ShowError("stable not support ascending operation, use descending");
                    asc = false;
                    ascCheck.Checked = false;
                }
            }

            String sql = this.GenerateSql(Globals.ThisAddIn.tdPersist.DB, tableName, selectedFields, from , to, asc, limitRows);
            
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

        private void FillListView()
        {
            this.ClearListView();
            String tablename = this.inputTextBox.Text;
            if (tablename == "")
            {
                Globals.ThisAddIn.tdUtil.ShowError("table name not input");
                return;
            }

            bool isMetrics = Globals.ThisAddIn.tdUtil.IsMetricsName(tablename);

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
                    if (!isMetrics && tag != "")
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

                if (isMetrics)
                {
                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = "tbname";
                    lvi.SubItems.Add("binary(32)");
                    this.rawListView.Items.Add(lvi);
                }

                this.rawListView.EndUpdate();
            }
        }

        private void ClearListView()
        {
            this.rawListView.BeginUpdate();
            this.rawListView.Clear();

            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = "Field Name";
            ch1.Width = 343;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);

            ColumnHeader ch2 = new ColumnHeader();
            ch2.Text = "Field Type";
            ch2.Width = 100;
            ch2.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch2);

            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.rawListView.CheckBoxes = true;

            this.rawListView.EndUpdate();
        }

        private void SelectListView()
        {
            foreach (ListViewItem lvi in this.rawListView.Items)
            {
                foreach (String field in Globals.ThisAddIn.tdPersist.detailSelectFields)
                {
                    if (field == lvi.Text)
                    {
                        lvi.Checked = true;
                    }
                }
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
        
        private String GenerateSql(String db, String table, String[] fields, String from, String to, bool asc, Int32 limitRows)
        {
            String sql = "select " + fields[0];
            for (int i = 1; i < fields.Length; ++i)
            {
                sql = sql + ", " + fields[i];
            }

            sql = sql + " from " + db + "." + table + " where _c0 >= '" + from + "' and _c0 < '" + to + "'";

            if (asc)
            {
                sql = sql + " order by _c0 asc";
            }

            if (limitRows != 0)
            {
                sql = sql + " limit " + limitRows.ToString();
            }

            return sql;
        }
    }
}

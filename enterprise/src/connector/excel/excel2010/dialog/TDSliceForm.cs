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
    public partial class TDSliceForm : TDForm
    {
        private bool isSelectOutputing = true;
        
        public TDSliceForm()
        {
            InitializeComponent();

            foreach (String sliceFillMethod in Globals.ThisAddIn.tdUtil.sliceFillMethods)
            {
                this.fillMethodCombox.Items.Add(sliceFillMethod);
            }
        }

        public override void Initialize()
        {
            this.inputTextBox.Text = Globals.ThisAddIn.tdPersist.sliceInput;
            this.FillListView();

            if (Globals.ThisAddIn.tdPersist.sliceTimestamp == 0)
            {
                DateTime begin = DateTime.Now.Date;
                fromTimePicker.Value = begin;
            }
            else
            {
                DateTime begin = new DateTime(Globals.ThisAddIn.tdPersist.sliceTimestamp);
                fromTimePicker.Value = begin;
            }

            this.fillMethodCombox.SelectedIndex = Globals.ThisAddIn.tdPersist.sliceFillMethodIndex;
            this.headsCheck.Checked = Globals.ThisAddIn.tdPersist.sliceShowHeads;
            this.timestampCheck.Checked = Globals.ThisAddIn.tdPersist.sliceDisplayAsTimestamp;

            this.SelectListView();
            this.outputTextbox.Text = Globals.ThisAddIn.tdPersist.sliceOutput;
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.sliceInput = this.inputTextBox.Text;
            Globals.ThisAddIn.tdPersist.sliceTimestamp = fromTimePicker.Value.Ticks;
            Globals.ThisAddIn.tdPersist.sliceFillMethodIndex = this.fillMethodCombox.SelectedIndex;

            Globals.ThisAddIn.tdPersist.sliceShowHeads = this.headsCheck.Checked;
            Globals.ThisAddIn.tdPersist.sliceDisplayAsTimestamp = this.timestampCheck.Checked;

            Globals.ThisAddIn.tdPersist.sliceSelectFields.Clear();
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                Globals.ThisAddIn.tdPersist.sliceSelectFields.Add(lvi.Text);
            }

            Globals.ThisAddIn.tdPersist.sliceOutput = this.outputTextbox.Text;
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

            String[] selectedFields = this.GetListViewCheckedItems();
            String from = fromTimePicker.Text;
            String fillMethod = Globals.ThisAddIn.tdUtil.sliceFillMethods[this.fillMethodCombox.SelectedIndex];
            String sql = this.GenerateSql(mtName, tableName, selectedFields, from, fillMethod);

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

                    if (fieldType == "BINARY" || fieldType == "NCHAR")
                    {
                        fieldType = fieldType + "(" + dataCols.GetValue(2).ToString() + ")";
                    }

                    if (!isFromMetrics && tag != "")
                    {
                        continue;
                    }

                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = field;
                    lvi.SubItems.Add(fieldType.ToLower());
                    this.rawListView.Items.Add(lvi);
                }

                if (isFromMetrics)
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
                
                if (showHeads)
                {
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[beginRow, col + beginCol].Value2 = heads.GetValue(col).ToString();
                    }
                    beginRow++;
                }

                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[row + beginRow, col + beginCol].Value2 = dataCols.GetValue(col).ToString();
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

        private String GenerateSql(String mt, String table, String[] fields, String from, String fillMethod)
        {
            String sql = "select " + fields[0];
            for (int i = 1; i < fields.Length; ++i)
            {
                sql = sql + ", " + fields[i];
            }

            if (mt == "")
            {
                sql = sql + " from " + Globals.ThisAddIn.tdPersist.DB + "." + table + " where _c0 == '" + from + "'";
            }
            else
            {
                String tableNames = table.Replace(",", "','");
                sql = sql + " from " + Globals.ThisAddIn.tdPersist.DB + "." + mt + " where tbname in('" + tableNames + "') and _c0=='" + from + "'"; ;
            }

            sql = sql + " fill(" + fillMethod + ")";

            return sql;
        }
    }
}

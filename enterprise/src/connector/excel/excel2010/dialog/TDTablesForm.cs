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
    public partial class TDTablesForm : TDForm
    {
        private bool isSelectOutputing = true;

        public TDTablesForm()
        {
            InitializeComponent();
         }

        public override void Initialize()
        {
            this.inputTextBox.Text = Globals.ThisAddIn.tdPersist.tablesInput;
            this.headsCheck.Checked = Globals.ThisAddIn.tdPersist.tablesShowHeads;
            this.basicinfoCheck.Checked = Globals.ThisAddIn.tdPersist.tablesShowBasicInfo;
            this.tagValuesCheck.Checked = Globals.ThisAddIn.tdPersist.tablesShowTagValues;
            this.outputTextbox.Text = Globals.ThisAddIn.tdPersist.tablesOutput;

            if (this.basicinfoCheck.Checked && this.tagValuesCheck.Checked)
            {
                this.tagValuesCheck.Checked = false;
            }
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.tablesShowHeads = this.headsCheck.Checked;
            Globals.ThisAddIn.tdPersist.tablesShowBasicInfo = this.basicinfoCheck.Checked;
            Globals.ThisAddIn.tdPersist.tablesShowTagValues = this.tagValuesCheck.Checked;
            Globals.ThisAddIn.tdPersist.tablesOutput = this.outputTextbox.Text;
            Globals.ThisAddIn.tdPersist.tablesInput = this.inputTextBox.Text;
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
                }
            }
        }

        private void InputSelect_Click(object sender, EventArgs e)
        {
            this.isSelectOutputing = false;
            this.GetFactory().StartSelect();
        }

        private void InputTextbox_MouseClick(object sender, MouseEventArgs e)
        {
            this.GetFactory().StopSelect();
        }

        private void TagValuesCheck_CheckedChanged(object sender, EventArgs e)
        {
            if (this.tagValuesCheck.Checked)
            {
                if (this.basicinfoCheck.Checked)
                {
                    this.basicinfoCheck.Checked = false;
                }
            }
        }

        private void BasicinfoCheck_CheckedChanged(object sender, EventArgs e)
        {
            if (this.basicinfoCheck.Checked)
            {
                if (this.tagValuesCheck.Checked)
                {
                    this.tagValuesCheck.Checked = false;
                }
            }
        }

        private void FilterTableCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            this.tablenameTextBox.Enabled = this.filterTableCheckBox.Checked;
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

            String mtName = this.inputTextBox.Text;
            if (mtName != "")
            {
                if (!Globals.ThisAddIn.tdUtil.IsMetricsName(mtName))
                {
                    Globals.ThisAddIn.tdUtil.ShowError("not a stable");
                    return;
                }
            }

            bool tagValuesCheck = this.tagValuesCheck.Checked;
            bool basicInfoCheck = this.basicinfoCheck.Checked;
            bool filterTableCheck = this.filterTableCheckBox.Checked;
            String filterTableName = this.tablenameTextBox.Text;

            Range outputRange = Globals.ThisAddIn.tdUtil.GetRange(outputTextbox.Text);
            if (outputRange == null)
            {
                Globals.ThisAddIn.tdUtil.ShowError("output columns not select");
                return;
            }

            if (tagValuesCheck && mtName != "")
            {
                JObject jo = this.DescribeTables(mtName);

                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                String sql = "select tbname";
                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String note = dataCols.GetValue(3).ToString();
                    if (note != "")
                    {
                        sql = sql + ", " + dataCols.GetValue(0).ToString();
                    }
                }
                sql = sql + " from " + Globals.ThisAddIn.tdPersist.DB + "." + mtName;

                JObject jo2 = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
                Globals.ThisAddIn.Application.ScreenUpdating = false;
                this.FillTableExcelWithTag(jo2, outputRange);
                Globals.ThisAddIn.Application.ScreenUpdating = true;
            }
            else
            {
                String sql = "show " + Globals.ThisAddIn.tdPersist.DB + ".tables";
                if (filterTableCheck && filterTableName != "")
                {
                    sql = sql + " like " + filterTableName;
                }
                JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
                Globals.ThisAddIn.Application.ScreenUpdating = false;
                this.FillTableExcel(jo, outputRange, mtName);
                Globals.ThisAddIn.Application.ScreenUpdating = true;
            }
        }
        
        private JObject DescribeTables(String tbname)
        {
            String sql = "describe " + Globals.ThisAddIn.tdPersist.DB + "." + tbname;
            return Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
        }

        private void FillTableExcel(JObject jo, Range range, String mtName)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;
                bool showBasicInfo = this.basicinfoCheck.Checked;
                bool showTagValues = this.tagValuesCheck.Checked;

                Excel.Worksheet activeWorksheet = Globals.ThisAddIn.Application.ActiveSheet;
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

                int tablesNum = 0;
                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String dataMtName = dataCols.GetValue(3).ToString();
                    if (mtName != dataMtName)
                    {
                        beginOffset--;
                        continue;
                    }

                    tablesNum++;
                    for (int col = 0; col < headLength; ++col)
                    {
                        activeWorksheet.Cells[rowArray[row + beginOffset], colArray[col]].Value2 = dataCols.GetValue(col).ToString();
                    }
                }

                if (tablesNum == 0)
                {
                    Globals.ThisAddIn.tdUtil.ShowError("no table was found.");
                    return;
                }
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }
        }

        private void FillTableExcelWithTag(JObject jo, Range range)
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
                    Globals.ThisAddIn.tdUtil.ShowError("no table was found.");
                    return;
                }
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }
        }

        private String GenerateSql()
        {
            return "show " + Globals.ThisAddIn.tdPersist.DB + ".tables";
        }
    }
}

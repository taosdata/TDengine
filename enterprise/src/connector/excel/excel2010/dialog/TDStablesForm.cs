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
    public partial class TDStablesForm : TDForm
    {
        public TDStablesForm()
        {
            InitializeComponent();
         }

        public override void Initialize()
        {
            this.outputTextbox.Text = Globals.ThisAddIn.tdPersist.stablesOutput;
            this.headsCheck.Checked = Globals.ThisAddIn.tdPersist.stablesShowHeads;
            this.basicinfoCheck.Checked = Globals.ThisAddIn.tdPersist.stablesShowBasicInfo;
        }

        public override void Save()
        {
            Globals.ThisAddIn.tdPersist.stablesOutput = this.outputTextbox.Text;
            Globals.ThisAddIn.tdPersist.stablesShowHeads = this.headsCheck.Checked;
            Globals.ThisAddIn.tdPersist.stablesShowBasicInfo = this.basicinfoCheck.Checked;
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
                String selectRange = Target.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
                this.outputTextbox.Text = Globals.ThisAddIn.tdUtil.SelectRangeByString(selectRange, TDFormSelectType.TD_FORM_SELECT_CELL);
            }
        }

        private void OutputTextbox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == System.Convert.ToChar(13))
            {
                this.outputTextbox.Text = Globals.ThisAddIn.tdUtil.SelectRangeByString(outputTextbox.Text, TDFormSelectType.TD_FORM_SELECT_CELL);
                e.Handled = true;
            }
        }

        private void OutputTextbox_MouseClick(object sender, MouseEventArgs e)
        {
            this.GetFactory().StopSelect();
        }
        
        private void OutputButton_Click(object sender, EventArgs e)
        {
           this.GetFactory().StartSelect();
        }
        
        private void Import_Click(object sender, EventArgs e)
        {
            this.GetFactory().StopSelect();
            String sql = this.GenerateSql();

            Range outputRange = Globals.ThisAddIn.tdUtil.GetRange(outputTextbox.Text);
            if (outputRange == null)
            {
                Globals.ThisAddIn.tdUtil.ShowError("output columns not select");
            }
            else
            {
                JObject jo = Globals.ThisAddIn.tdHttp.DoRequest(sql, false);
                if (jo != null)
                {
                    Globals.ThisAddIn.Application.ScreenUpdating = false;
                    this.FillExcel(jo, outputRange);
                    Globals.ThisAddIn.Application.ScreenUpdating = true;
                    //this.GetFactory().CloseForm();
                }
            }
        }

        private void FillExcel(JObject jo, Range range)
        {
            try
            {
                bool showHeads = this.headsCheck.Checked;
                bool showBasicInfo = this.basicinfoCheck.Checked;
                
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
                    Globals.ThisAddIn.tdUtil.ShowError("no stable was found.");
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
            return "show " + Globals.ThisAddIn.tdPersist.DB + ".stables";
        }
    }
}

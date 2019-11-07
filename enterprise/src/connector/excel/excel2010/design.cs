using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Office.Tools.Ribbon;


using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Workbook = Microsoft.Office.Interop.Excel.Workbook;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;

using System.Windows.Forms;
using Microsoft.Office.Interop.Excel;

namespace excel2010
{


    public partial class design
    {
        private void Ribbon1_Load(object sender, RibbonUIEventArgs e)
        {
        }

        private void StablesButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_STABLES);
        }

        private void TablesButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_TABLES);
        }

        private void AggButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_AGGREGATION);
        }
        
        private void DetailButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_DETAILS);
        }

        private void SliceButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_SLICE);
        }

        private void ConnectButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_CONNECT);
        }

        private void AboutButton_Click(object sender, RibbonControlEventArgs e)
        {
            Globals.ThisAddIn.tdForms.ShowForm(TDFormType.TD_FORM_ABOUT);
        }
    }
}

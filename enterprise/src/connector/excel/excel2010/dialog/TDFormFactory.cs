using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Workbook = Microsoft.Office.Interop.Excel.Workbook;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using System.ComponentModel;
using Microsoft.Office.Interop.Excel;

namespace excel2010
{
    public enum TDFormType
    {
        TD_FORM_STABLES = 0,
        TD_FORM_TABLES,
        TD_FORM_AGGREGATION,
        TD_FORM_DETAILS,
        TD_FORM_SLICE,
        TD_FORM_CONNECT,
        TD_FORM_ABOUT,
        TD_FORM_MAX
    }

    public enum TDFormSelectType
    {
        TD_FORM_SELECT_CELL,
        TD_FORM_SELECT_ROW,
        TD_FORM_SELECT_COLUMN,
        TD_FORM_SELECT_AREA
    }
    
    public class TDFormFactory
    {
        public TDFormFactory()
        {
            TDFormParent parent = new TDFormParent();
            forms[(int)TDFormType.TD_FORM_STABLES] = new TDStablesForm();
            forms[(int)TDFormType.TD_FORM_TABLES] = new TDTablesForm();
            forms[(int)TDFormType.TD_FORM_AGGREGATION] = new TDAggregationForm();
            forms[(int)TDFormType.TD_FORM_DETAILS] = new TDDetailsForm();
            forms[(int)TDFormType.TD_FORM_SLICE] = new TDSliceForm();
            forms[(int)TDFormType.TD_FORM_CONNECT] = new TDConnectForm();
            forms[(int)TDFormType.TD_FORM_ABOUT] = new TDAboutForm();

            foreach (TDForm form in this.forms)
            {
                form.StartPosition = FormStartPosition.CenterScreen;
                form.ShowInTaskbar = false;
                form.TopMost = false;
                form.ControlBox = true;
                form.SetParent(parent);
                form.SetFactory(this);
            }

            Globals.ThisAddIn.Application.SheetChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetChangeEventHandler(SheetChange);
            Globals.ThisAddIn.Application.SheetSelectionChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetSelectionChangeEventHandler(SheetSelectionChange);
        }

        public void ShowForm(TDFormType formType)
        {
            foreach (TDForm form in this.forms)
            {
                form.CustomClose();
            }
            
            this.curForm = forms[(int)formType];
            this.curForm.CustomShow();
        }

        public void CloseForm()
        {
            foreach (TDForm form in this.forms)
            {
                form.CustomClose();
            }

            this.curForm = null;
        }
        
        private void SheetChange(object Sh, Range Target)
        {}

        private void SheetSelectionChange(object Sh, Range Target)
        {
            if (!this.selecting || this.curForm == null)
            {
                return;
            }

            try
            {
                Worksheet sheet = Sh as Worksheet;
                this.curForm.SheetSelectionChange(sheet, Target);
            }
            catch (Exception e)
            {
                Globals.ThisAddIn.tdUtil.ShowException(e);
            }
            finally { }
        }
        
        public void StartSelect()
        {
            this.selecting = true;

            //try
            //{
            //    var select = Globals.ThisAddIn.Application.Selection;
            //    Range selectRange = select as Range;
            //    SheetSelectionChange(Globals.ThisAddIn.Application.ActiveSheet, selectRange);
            //}
            //catch (Exception)
            //{ }
            //finally { }

            //this.selecting = true;
        }

        public void StopSelect()
        {
            this.selecting = false;
        }
        
        public TDForm[] forms = new TDForm[(int)TDFormType.TD_FORM_MAX];
        private TDForm curForm = null;
        private bool selecting = false;
    }

    public class TDForm : Form
    {
        protected override void OnClosing(CancelEventArgs e)
        {
            this.factory.StopSelect();
            this.Hide();
            e.Cancel = true;
        }

        public virtual void Initialize()
        {}

        public virtual void SheetSelectionChange(Worksheet sheet, Range Target)
        {}

        public virtual void Save()
        {}

        public void CustomShow()
        {
            if (!this.isInitialized)
            {
                this.isInitialized = true;
                this.Initialize();
            }
            this.Show(parent);
        }

        public void CustomClose()
        {
            this.factory.StopSelect();
            this.Hide();
        }

        public void SetParent(TDFormParent parent)
        {
            this.parent = parent;
        }

        public void SetFactory(TDFormFactory factory)
        {
            this.factory = factory;
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
        }

        public TDFormFactory GetFactory()
        {
            return this.factory;
        }

        private TDFormParent parent;
        private TDFormFactory factory;
        public bool isInitialized = false;
    }

    public class TDFormParent : IWin32Window
    {
        public TDFormParent()
        {
            this.handle = (IntPtr)Globals.ThisAddIn.Application.Hwnd;
        }

        IntPtr IWin32Window.Handle => handle;

        private IntPtr handle;
    }
}

using System;
using System.Windows.Forms;
using System.Runtime.InteropServices;
using ExcelDna.Integration.CustomUI;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Microsoft.Office.Interop.Excel;
using ExcelDna.Integration;

namespace TDengineExcelPlugins
{
    //https://msdn.microsoft.com/en-us/library/Microsoft.Office.Tools.CustomTaskPane(v=vs.80).aspx  CustomTaskPane Class
    //https://msdn.microsoft.com/zh-cn/library/microsoft.office.tools.customtaskpane(v=vs.100).aspx CustomTaskPane 接口 office2010
    //https://msdn.microsoft.com/zh-cn/library/microsoft.office.tools.customtaskpanecollection(v=VS.100).aspx CustomTaskPaneCollection 接口
    //https://msdn.microsoft.com/zh-cn/library/bb608620.aspx Managing Custom Task Panes in Multiple Application Windows

    //https://msdn.microsoft.com/en-us/library/aa942846.aspx How to: Add a Custom Task Pane to an Application


    //https://msdn.microsoft.com/en-us/library/aa942864.aspx Custom Task Panes
    /*
    Controlling the Task Pane in Multiple Windows 
    
    Custom task panes are associated with a document frame window, which presents a view of a document or item to the user.
    The task pane is visible only when the associated window is visible.
    
    To determine which window displays the custom task pane, 
    use the appropriate Add method overload when you create the task pane:
    1、To associate the task pane with the active window, use the CustomTaskPaneCollection.Add(UserControl, String) method.
    2、To associate the task pane with a document that is hosted by a specified window, use the CustomTaskPaneCollection.Add(UserControl, String, Object) method.
    
    Some Office applications require explicit instructions for when to create or display your task pane when more than one window is open. 
    This makes it important to consider where to instantiate the custom task pane in your code to ensure that the task pane appears with the appropriate documents or items in the application. 
    For more information, see Managing Custom Task Panes in Application Windows.
     */

    //http://www.cnblogs.com/yangecnu/archive/2013/10/18/3375338.html Excel 自定义任务窗体
    //http://blogs.msdn.com/b/vsto/archive/2010/02/02/add-a-custom-task-pane-to-project-2010-norm-estabrook.aspx Add a Custom Task Pane to Project 2010 (Norm Estabrook)

    //考虑到 Excel2013改成了single document interface (SDI)，因此需要在application事件中处理任务窗格，以保证在当前窗体中能够显示。
    //https://msdn.microsoft.com/en-us/library/office/dn251093(v=office.15).aspx#odc_xl15_ta_ProgrammingtheSDIinExcel2013_TaskPanes

    //http://www.jkp-ads.com/Articles/keepuserformontop02.asp  Keeping Userforms On Top Of SDI Windows In Excel 2013 And Up
    //https://www.add-in-express.com/creating-addins-blog/2013/02/28/excel2013-single-document-interface-task-panes/
    /// <summary>
    /// 任务窗格管理类
    /// </summary>
    internal static class TDFactory
    {
        static public Excel.Application Application()
        {
            if (_application != null) return _application;

            try
            {
                _application = (Excel.Application)Marshal.GetActiveObject("Excel.Application");
                _application.SheetChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetChangeEventHandler(SheetChange);
                _application.SheetSelectionChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetSelectionChangeEventHandler(SheetSelectionChange);
                return _application;
            }
            catch (Exception) {
                try
                {
                    _application = ExcelDnaUtil.Application as Excel.Application;
                    _application.SheetChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetChangeEventHandler(SheetChange);
                    _application.SheetSelectionChange += new Microsoft.Office.Interop.Excel.AppEvents_SheetSelectionChangeEventHandler(SheetSelectionChange);
                    return _application;
                }
                catch (Exception) { }
                finally { }
            }
            finally { }
            return null;
        }

        static public Excel.Application _application = null;
        static public TDUtil Util;
        static public TDPersist Persist;
        static public TDHttp Http;
        static public TDFormula Formula;
        static public TDExcel Excel;
            
        static public TDForm[] forms = new TDForm[(int)TDFormType.TD_FORM_MAX];
        static private TDForm curForm = null;
        
        static public void Initialize()
        {
            Util = new TDUtil();
            Persist = TDPersist.Load();
            Http = new TDHttp();
            Formula = new TDFormula();
            Excel = new TDExcel();
 
            Http.DoLoginSilent();
        }

        static public void ShowForm(TDFormType formType)
        {
            curForm = forms[(int)formType];
            if (curForm == null)
            {
                forms[(int)TDFormType.TD_FORM_STABLES] = new TDForm(typeof(TDStablesForm), "Query STables");
                forms[(int)TDFormType.TD_FORM_TABLES] = new TDForm(typeof(TDTablesForm), "Query Tables");
                forms[(int)TDFormType.TD_FORM_AGGREGATION] = new TDForm(typeof(TDAggregationForm), "Aggregation Query");
                forms[(int)TDFormType.TD_FORM_DETAILS] = new TDForm(typeof(TDDetailsForm), "Query Detail Data");
                forms[(int)TDFormType.TD_FORM_SLICE] = new TDForm(typeof(TDSliceForm), "Query Slice Data");
                forms[(int)TDFormType.TD_FORM_FAGGREGATION] = new TDForm(typeof(TDFAggregationForm), "Formula of Aggregation");
                forms[(int)TDFormType.TD_FORM_FSLICE] = new TDForm(typeof(TDFSliceForm), "Formula of Slice Data");
                forms[(int)TDFormType.TD_FORM_FCALC] = new TDForm(typeof(TDFCalculateForm), "Calculate all the Formulas");
                forms[(int)TDFormType.TD_FORM_CONNECT] = new TDForm(typeof(TDConnectForm), "Connect to TDengine");
                forms[(int)TDFormType.TD_FORM_ABOUT] = new TDForm(typeof(TDAboutForm), "About this Plugin");
                curForm = forms[(int)formType];
            }

            foreach (TDForm form in forms)
            {
                if (curForm != form) form.CustomClose();
            }
            
            curForm.CustomShow();
        }

        static public void CloseForm()
        {
            foreach (TDForm form in forms)
            {
                form.CustomClose();
            }

            curForm = null;
        }
        
        static private void SheetChange(object Sh, Range Target)
        { }

        static private void SheetSelectionChange(object Sh, Range Target)
        { }

        static public void StartUpdate()
        {
            lastCalcMethod = TDFactory.Application().Calculation;
            TDFactory.Application().Calculation = Microsoft.Office.Interop.Excel.XlCalculation.xlCalculationManual;
            TDFactory.Application().ScreenUpdating = false;
        }

        static public void EndUpdate()
        {
            TDFactory.Application().Calculation = lastCalcMethod;
            TDFactory.Application().ScreenUpdating = true;
        }

        static private Microsoft.Office.Interop.Excel.XlCalculation lastCalcMethod;
    }

    public enum TDFormType
    {
        TD_FORM_STABLES = 0,
        TD_FORM_TABLES,
        TD_FORM_AGGREGATION,
        TD_FORM_DETAILS,
        TD_FORM_SLICE,
        TD_FORM_FAGGREGATION,
        TD_FORM_FSLICE,
        TD_FORM_FCALC,
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

    [ComVisible(true)]
    public class TDControl : UserControl
    {
        public virtual void Initialize()
        { }
        
        public virtual void Save()
        { }

        public virtual void Start()
        { }

        public void SetCtp(CustomTaskPane ctp)
        {
            this.ctp = ctp;
            ctp.Width = 500;
        }

        public CustomTaskPane ctp;
        public bool isInitialized = false;
    }

    public class TDForm : UserControl
    {
        private CustomTaskPane ctp;
        public TDControl form;

        public TDForm(System.Type formType, String formTitle)
        { 
            ctp = CustomTaskPaneFactory.CreateCustomTaskPane(formType, formTitle);
            ctp.DockPosition = MsoCTPDockPosition.msoCTPDockPositionRight;
            ctp.DockPositionStateChange += DockPositionStateChange;
            ctp.VisibleStateChange += VisibleStateChange;
            ctp.Visible = false;
            form = ctp.ContentControl as TDControl;
            form.SetCtp(ctp);
        }

        public void CustomShow()
        {
            if (!form.isInitialized)
            {
                form.isInitialized = true;
                form.Initialize();
            }

            //bool lastVisible = ctp.Visible;
            
            if (!ctp.Visible) form.Start();
            ctp.Visible = true;
        }

        public void CustomClose()
        {
            ctp.Visible = false;
        }

        void VisibleStateChange(CustomTaskPane CustomTaskPaneInst)
        { }

        void DockPositionStateChange(CustomTaskPane CustomTaskPaneInst)
        { }
    }
}

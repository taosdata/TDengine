namespace excel2010
{
    public partial class ThisAddIn
    {
        public TDFormFactory tdForms;
        public TDHttp tdHttp;
        public TDUtil tdUtil;
        public TDPersist tdPersist;

        private void ThisAddIn_Startup(object sender, System.EventArgs e)
        {
            this.tdUtil = new TDUtil();
            this.tdPersist = TDPersist.Load();
            this.tdHttp = new TDHttp();
            this.tdForms = new TDFormFactory();
            
            this.Application.WorkbookBeforeSave += new Microsoft.Office.Interop.Excel.AppEvents_WorkbookBeforeSaveEventHandler(this.Application_WorkbookBeforeSave);
        }

        private void ThisAddIn_Shutdown(object sender, System.EventArgs e)
        {
            this.tdPersist.Save();
        }

        void Application_WorkbookBeforeSave(Microsoft.Office.Interop.Excel.Workbook Wb, bool SaveAsUI, ref bool Cancel)
        {}
       
        #region VSTO 生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要修改
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InternalStartup()
        {
            this.Startup += new System.EventHandler(ThisAddIn_Startup);
            this.Shutdown += new System.EventHandler(ThisAddIn_Shutdown);
        }
        
        #endregion
    }
}

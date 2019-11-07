using ExcelDna.Integration;

namespace TDengineExcelPlugins
{
    class XllAddIn : IExcelAddIn
    {
        //https://msdn.microsoft.com/zh-cn/library/bb687860(v=office.15)
        //Callback function that must be implemented and exported by every valid XLL. The xlAutoOpen function is the recommended place from where to register XLL functions and commands, initialize data structures, customize the user interface, and so on.
        public void AutoOpen()
        {
            TDFactory.Initialize();
        }
        //https://msdn.microsoft.com/zh-cn/library/bb687830.aspx
        //Called by Microsoft Excel whenever the XLL is deactivated. The add-in is deactivated when an Excel session ends normally. The add-in can be deactivated by the user during an Excel session, and this function will be called in that case.
        //Excel does not require an XLL to implement and export this function, although it is advisable so that your XLL can unregister functions and commands, release resources, undo customizations, and so on.If functions and commands are not explicitly unregistered by the XLL, Excel does this after calling the xlAutoClose function.
        public void AutoClose()
        {
        }
    }
}

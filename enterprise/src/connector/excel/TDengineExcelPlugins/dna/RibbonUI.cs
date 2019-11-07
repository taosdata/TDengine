using ExcelDna.Integration;
using ExcelDna.Integration.CustomUI;
using System;
using System.Runtime.InteropServices;
using System.Windows.Forms;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// Load custom Excel Fluent/Ribbon
    /// </summary>
    [ComVisible(true)]
    public class RibbonUI : ExcelRibbon
    {
        private static IRibbonUI customRibbon;             //记录IRibbonUI对象

        #region Fluent/Ribbon UI
        //https://blog.csdn.net/ITTechnologyHome/article/details/53891087             //VisualStudio2017集成GitHub

        //https://msdn.microsoft.com/en-us/library/aa722523(v=office.12).aspx         //Ribbon函数回调定义
        //https://msdn.microsoft.com/zh-cn/library/office/ee691833(v=office.14).aspx  //Office 2010 Backstage 视图介绍

        /// <summary>
        /// ribbon callback, get IRibbonUI object.
        /// </summary>
        public void ribbonLoaded(IRibbonUI ribbon)
        {
            customRibbon = ribbon;
        }

        /// <summary>
        /// read CustomUI.xml, xml file must be UTF-8 encode and Embedded resources.
        /// </summary>
        public override string GetCustomUI(string uiName)
        {
            string ribbonxml = string.Empty;
            try
            {
                if (ExcelDnaUtil.ExcelVersion == 12)
                    ribbonxml = ResourceHelper.GetResourceText("CustomUI12.xml");

                else
                    ribbonxml = ResourceHelper.GetResourceText("CustomUI14.xml");
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return ribbonxml;
        }

        /// <summary>
        /// Ribbon callback，load image in XML element
        /// </summary>
        public override object LoadImage(string imageId)
        {
            return ResourceHelper.GetResourceBitmap(imageId);
        }
        
        /// <summary>
        /// ribbon callback
        /// </summary>
        public stdole.IPictureDisp Button_getImage(IRibbonControl control)
        {
            stdole.IPictureDisp pictureDisp = null;

            switch (control.Id)
            {
                case "stablesButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".stables.png"));
                    break;
                case "tablesButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".tables.png"));
                    break;
                case "faggButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".fagg.png"));
                    break;
                case "fsliceButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".fslice.png"));
                    break;
                case "fcalcButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".fcalc.png"));
                    break;
                case "aggButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".agg.png"));
                    break;
                case "detailButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".detail.png"));
                    break;
                case "sliceButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".slice.png"));
                    break;
                case "connectButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".connect.png"));
                    break;
                case "aboutButton":
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".about.png"));
                    break;
                default:
                    pictureDisp = Image2stdoleIPictureDisp.ImageToPictureDisp(ResourceHelper.GetResourceBitmap(".others.png"));
                    break;
            }
            return pictureDisp;
        }
       
        #endregion Fluent/Ribbon UI

       
        public void StablesButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_STABLES);
        }

        public void TablesButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_TABLES);
        }

        public void AggButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_AGGREGATION);
        }

        public void DetailButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_DETAILS);
        }

        public void SliceButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_SLICE);
        }

        public void FAggButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_FAGGREGATION);
        }

        public void FSliceButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_FSLICE);
        }

        public void FCalcButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_FCALC);
        }

        public void ConnectButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_CONNECT);
        }

        public void AboutButton_Click(IRibbonControl control)
        {
            TDFactory.ShowForm(TDFormType.TD_FORM_ABOUT);
        }
    }
}

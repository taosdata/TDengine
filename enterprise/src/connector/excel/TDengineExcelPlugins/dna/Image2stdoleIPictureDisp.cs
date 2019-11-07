using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TDengineExcelPlugins
{
    internal class Image2stdoleIPictureDisp : System.Windows.Forms.AxHost
    {
        private Image2stdoleIPictureDisp() : base(null) { }

        static public stdole.IPictureDisp ImageToPictureDisp(System.Drawing.Image image)
        {
            return (stdole.IPictureDisp)GetIPictureDispFromPicture(image);
        }

        static public System.Drawing.Image PictureDispToImage(stdole.IPictureDisp pictureDisp)
        {
            return GetPictureFromIPicture(pictureDisp);
        }
    }
}

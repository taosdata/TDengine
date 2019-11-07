using System.Reflection;
using System.IO;

namespace TDengineExcelPlugins
{
    class ResourceHelper
    {
        //Using Custom Images in your Office Ribbon
        //http://social.msdn.microsoft.com/forums/zh-cn/vsto/thread/7074781a-54dc-4d6a-884d-6de5df0be7e7?persist=true

        //获取资源图片，文件要在属性-生成操作-嵌入资源
        internal static System.Drawing.Bitmap GetResourceBitmap(string resourceName)
        {
            System.Drawing.Bitmap image = null;

            //String projectName = Assembly.GetExecutingAssembly().GetName().Name.ToString();获取项目名
            string[] resources = Assembly.GetExecutingAssembly().GetManifestResourceNames();    //获取全部资源

            //office.png 属性-生成操作-嵌入的资源，然后才能在GetManifestResourceStream被找到。
            //asm.GetManifestResourceStream("项目命名空间.资源文件所在文件夹名.资源文件名"); GetManifestResourceStream("CSharpAddIn.Img.office.png");  

            string extension = System.IO.Path.GetExtension(resourceName).ToLower();             //扩展名

            foreach (string resource in resources)
            {
                if (resource.EndsWith(resourceName))
                {
                    System.IO.Stream streamImg = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource);
                    switch (extension)
                    {
                        //http://blogs.msdn.com/b/jensenh/archive/2006/11/27/ribbonx-image-faq.aspx
                        case ".ico":
                            image = new System.Drawing.Icon(streamImg).ToBitmap();
                            break;

                        case ".png":
                        case ".jpg":
                        case ".bmp":
                        default:
                            image = new System.Drawing.Bitmap(streamImg);
                            image.MakeTransparent();
                            break;
                    }
                    streamImg.Close();
                    break;
                }
            }
            return image;
        }

        //获取资源文本文件，文件要在属性-生成操作-嵌入资源
        internal static string GetResourceText(string resourceName)
        {
            string text = string.Empty;

            Assembly assm = Assembly.GetExecutingAssembly();
            string[] resources = assm.GetManifestResourceNames();    //获取全部资源

            foreach (string resource in resources)
            {
                if (resource.EndsWith(resourceName))
                {
                    System.IO.Stream streamText = assm.GetManifestResourceStream(resource);
                    StreamReader reader = new StreamReader(streamText);
                    text = reader.ReadToEnd();
                    reader.Close();
                    streamText.Close();
                    break;
                }
            }
            return text;
        }
    }
}


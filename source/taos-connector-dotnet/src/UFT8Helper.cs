using System;
using System.Runtime.InteropServices;
using System.Text;

namespace TDengineHelper
{
    public struct UTF8PtrStruct
    {
        /// <summary>
        /// The "pointer" point to the input string encoding with UTF-8 in unmanaged memory
        /// </summary>
        public IntPtr utf8Ptr { get; set; }
       
        /// <summary>
        /// Get the input string's length encoding with UTF-8
        /// </summary>
        public int utf8StrLength { get; set; }

        /// <summary>
        /// Allocate unmanaged memory for the incoming string based on difference .NET FrameWorks OR .NET standards encoding with UTF-8
        /// </summary>
        /// <param name="str"> input string </param>
        public UTF8PtrStruct(string str)
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP1_1_OR_GREATER
            utf8StrLength = Encoding.UTF8.GetByteCount(str);
            utf8Ptr = Marshal.StringToCoTaskMemUTF8(str);

#else

            var utf8Bytes = Encoding.UTF8.GetBytes(str);
            utf8StrLength = utf8Bytes.Length;
            byte[] targetUtf8Bytes = new byte[utf8StrLength + 1];
            utf8Bytes.CopyTo(targetUtf8Bytes, 0);

            utf8Ptr = Marshal.AllocHGlobal(utf8StrLength + 1);
            Marshal.Copy(targetUtf8Bytes, 0, utf8Ptr, utf8StrLength + 1);
#endif
        }

        /// <summary>
        ///  Remember to free the "pointer" unmanaged memory after using.
        /// </summary>
        public void UTF8FreePtr()
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP1_1_OR_GREATER
            Marshal.FreeCoTaskMem(utf8Ptr);
#else 
            Marshal.FreeHGlobal(utf8Ptr);
#endif
        }

    }
    public static class StringHelper
    {
        public static string PtrToStringUTF8(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                return string.Empty;
            }
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP1_1_OR_GREATER
            return Marshal.PtrToStringUTF8(ptr);
#else
            int len = 0;
            while (Marshal.ReadByte(ptr, len) != 0)
                len++;

            byte[] buffer = new byte[len];
            Marshal.Copy(ptr, buffer, 0, len);

            return Encoding.UTF8.GetString(buffer);
#endif
        }
        public static string PtrToStringUTF8(IntPtr ptr, int byteLen)
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP1_1_OR_GREATER
            return Marshal.PtrToStringUTF8(ptr,byteLen);
#else
            byte[] buffer = new byte[byteLen];
            Marshal.Copy(ptr, buffer, 0, byteLen);

            return Encoding.UTF8.GetString(buffer);
#endif
        }
    }
    

}

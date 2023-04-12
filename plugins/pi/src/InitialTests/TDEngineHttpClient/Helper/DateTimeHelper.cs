using System;

namespace TDEngineHttpClient.Helper
{ 
    internal static class DateTimeHelper
    {
        internal static string ToUtcTimeString(this DateTime dt)
        {
            return dt.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff");
        }
    }
}

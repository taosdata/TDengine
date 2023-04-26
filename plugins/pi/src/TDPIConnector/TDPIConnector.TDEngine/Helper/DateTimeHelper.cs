using System;

namespace TDPIConnector.TDEngine.Helper
{ 
    internal static class DateTimeHelper
    {
        internal static string ToUtcTimeString(this DateTime dt)
        {
            return dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }
    }
}

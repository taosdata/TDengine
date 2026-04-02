using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace TDEngineDR.TDEngineClient.Models
{
    public static class StringHelper
    {
        public static string ToDatabaseName(this string s)
        {
            return s.ToLower().Replace(" ", "_");
        }

        internal static string ToUtcTimeString(this DateTime dt)
        {
            return dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }

        public static async Task<string> ToStringResponse(this HttpResponseMessage response)
        {
            return await response.Content.ReadAsStringAsync();
        }
    }
}

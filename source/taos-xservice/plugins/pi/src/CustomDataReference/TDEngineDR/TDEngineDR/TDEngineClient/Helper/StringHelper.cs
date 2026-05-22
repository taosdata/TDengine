using System;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace TDEngineDR.TDEngineClient.Models
{
    public static class StringHelper
    {
        private static readonly Regex ValidIdentifier = new Regex(@"^[A-Za-z0-9_]+$", RegexOptions.Compiled);

        public static string ToDatabaseName(this string s)
        {
            return s.ToLower().Replace(" ", "_");
        }

        public static string SanitizeIdentifier(this string identifier)
        {
            if (string.IsNullOrEmpty(identifier) || !ValidIdentifier.IsMatch(identifier))
            {
                throw new ArgumentException($"Invalid SQL identifier: '{identifier}'");
            }
            return identifier;
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

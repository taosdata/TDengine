namespace TDPIConnector.TDEngine.Helper
{
    public static class StringHelper
    {
        public static string ToTDEngineNamingPattern(this string s)
        {
            string newString = string.Empty;
            s = s.ToLower();
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (char.IsLetterOrDigit(c))
                {
                    newString += c;
                }
                else
                {
                    newString += "_";
                }
            }

            if (char.IsDigit(newString[0]))
            {
                newString = "_" + newString.Substring(1, newString.Length - 1);
            }
            return newString;
        }
        public static string ToTDEngineNamingRawPattern(this string s)
        {
            return "`" + s + "`";
        }
    }
}

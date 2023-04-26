#define CLOUD_LICENSE_ONLY_DISABLED

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDEngineTableFormat
    {
        public static string PointValColomn()
        {
            return $"val";
        }
        public static string PointStatusColomn()
        {
            return $"status";
        }

        public static string AFValColomn(string name)
        {
            return $"{name}_val";
        }
        public static string AFStatusColomn(string name)
        {
            return $"{name}_status";
        }
    }

    public static class TaosxConstants
    {
        public const string TYPE = "__type__";
        public const string TABLES = "__tables__";
        public const string ATTRS = "__attrs__";
        public const string RECORDS = "__records__";
        public const string TABLENAME = "__table_name__";
        public const string LOCATIONTAG = "location";
    }
}

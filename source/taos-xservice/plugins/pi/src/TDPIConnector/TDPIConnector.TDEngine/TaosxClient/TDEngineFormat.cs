#define CLOUD_LICENSE_ONLY_DISABLED

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TDEngineTableFormat
    {
        public static string PointValColomn()
        {
            return $"value";
        }
        public static string PointStatusColomn()
        {
            return $"status";
        }

        public static string AFValColomn(in string name)
        {
            return name;
        }
        public static string AFStatusColomn(in string name)
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
        public const string CONTROL = "__control__";
        public const string TABLENAME = "__table_name__";
        public const string LOCATIONTAG = "location";
        public const string POINTID = "point_id";
        public const string ELEMENTID = "element_id";
        public const string POINTNAME = "point_name";
        public const string ELEMENTNAME = "element_name";
    }
}

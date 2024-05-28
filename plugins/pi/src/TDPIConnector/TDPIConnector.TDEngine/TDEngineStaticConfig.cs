using System;

namespace TDPIConnector.TDEngine
{
    public static class StaticConfig
    {
        public static TDEngineStaticConfig Default = new TDEngineStaticConfig();
    }

    public class TDEngineStaticConfig
    {
        public string AFTreeTagName { get; set; } = "path";
        public string ElementCategories { get; set; } = "categories";
        public string PointPath { get; set; } = "path";

        public string ElementsPathForPoint { get; set; } = "element_paths";
        public string PITablesPrefix { get; set; }
        public string TDDataBase { get; set; }
        public int MaxWaitLen { get; set; } = 1000;
        public int HttpMaxRetryTime { get; internal set; } = 3;
        public int HttpMaxRetryTimes { get; private set; }
        public bool ForBackfill { get; private set; } = false;

        public TDEngineStaticConfig() { 
        }
        public TDEngineStaticConfig SetAFTreeTagName(string tagName) {
            AFTreeTagName = tagName;
            return this;
        }

        public TDEngineStaticConfig SetPITablesPrefix(string prefix)
        {
            PITablesPrefix = prefix;
            return this;
        }

        public TDEngineStaticConfig SetMaxWaitLen(int maxWaitLen)
        {
            MaxWaitLen = maxWaitLen;
            return this;
        }

        public TDEngineStaticConfig SetHttpMaxTryTimes(int httpMaxRetryTimes)
        {
            HttpMaxRetryTimes = httpMaxRetryTimes;
            return this;
        }

        public TDEngineStaticConfig SetTDDatabase(string tdDataBase)
        {
            TDDataBase = tdDataBase;
            return this;
        }

        public TDEngineStaticConfig SetBackfill(bool forBackfill)
        {
            ForBackfill = forBackfill;
            return this;
        }
    }
}

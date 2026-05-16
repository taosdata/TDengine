namespace TDPIConnector.Core
{
    public class StandbyManager
    {
        private static StandbyManager instance;

        public static StandbyManager Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new StandbyManager();
                }
                return instance;
            }
        }

        public StandbyManager()
        {
            TDEngineConnectionErrorCount = 0;
        }

        public bool StandByModeEnabled
        {
            get
            {
                return PIConnectionError || TDEngineConnectionError;
            }
        }
        public bool PIConnectionError { get; set; }

        public int PIConnectionErrorCount { get; private set; }
        public bool TDEngineConnectionError { get; set; }
        public int TDEngineConnectionErrorCount { get; set; }   

        internal void ReportPIConnectionSuccess()
        {
            PIConnectionErrorCount = 0;
            PIConnectionError = false;
        }
        internal void ReportPIConnectionFailure()
        {
            PIConnectionErrorCount++;
            if (PIConnectionErrorCount == 3)
            {
                PIConnectionError = true;
                PIConnectionErrorCount = 0;
            }
        }
        internal void ReportTDEngineConnectionSuccess()
        {
            TDEngineConnectionError = false;
            TDEngineConnectionErrorCount = 0;
        }

        internal void ReportTDEngineConnectionFailure()
        {
            TDEngineConnectionErrorCount++;
            if (TDEngineConnectionErrorCount == 3)
            {
                TDEngineConnectionError = true;
                TDEngineConnectionErrorCount = 0;
            }
          
        }


    }
}

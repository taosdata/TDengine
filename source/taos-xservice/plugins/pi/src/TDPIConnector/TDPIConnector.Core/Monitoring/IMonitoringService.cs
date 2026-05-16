using System;
using System.Collections.Generic;
using TDPIConnector.Core.Monitoring.Models;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core.Monitoring
{
    public interface IMonitoringService
    {
        void PublishLastAFEvent(List<AFDataPipeEventWrapper> dpEvents);
        void PublishLastPIEvent(List<AFDataPipeEventWrapper> dpEvents);
        void PublishPIEvent(AFDataPipeEventWrapper dpEvent);
        void PublishAFEvent(AFDataPipeEventWrapper dpEvent);
        void PublishTDEngineHttpResponse(TDEngineHttpResponseSummary httpResponse);
        void PublishPIConnectionStatus(PIConnection piConnectionInfo);

        MonitoringInfo GetMonitoringInfo();
        void PublishTDEngineServerVersion(string version);

        bool Enabled { get; set; }

        void PublishPIException(Exception e);
        List<ExceptionSummary> GetExceptions();
        void PublishTDException(Exception ex);
    }
}

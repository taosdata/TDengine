using log4net;
using PISDK;
using System;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI2
{
    public class DateTimeWrapper
    {
        public DateTime Value { get; set; }
    }
    public class PIServerManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly string piServerName;
        private Server piServer;

        public PIServerManager(string piServerName)
        {
            this.piServerName = piServerName;
        }

        public void Connect()
        {
            try
            {
                piServer = new PISDK.PISDK().Servers[piServerName];
                piServer.Open();
            }
            catch (Exception e)
            {
                log.Error($"Error connecting to PI Server.", e);
                PISystemConnectionException piSystemConnectionException = new PISystemConnectionException(e);
                throw piSystemConnectionException;
            }
        }

        public void CreatePoint(string pointName) {
            if (!CheckPointExist(pointName))
            {
                PIPoint piPoint = piServer.PIPoints.Add(pointName, "classic", PointTypeConstants.pttypFloat64, null);
                log.Info($"{pointName} has been created.");
            }
            else {
                log.Info($"{pointName} has been exist,  not need to create.");
            }
        }

        public void UpdataPoint(string pointName, DateTime ts, double value)
        {
            if (CheckPointExist(pointName))
            {
                PIPoint piPoint = piServer.PIPoints[pointName];
                piPoint.Data.UpdateValue(value, ts);
                log.Info($"{pointName} = {value} at {ts}.");
            }
            else
            {
                log.Error($"{pointName} not exist when update point.");
            }
        }

        public bool CheckPointExist(string pointName)
        {
            PIPoint piPoint;
            try
            {
                piPoint = piServer.PIPoints[pointName];
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("not exist")) {
                    return false;
                } else {
                    throw ex;
                }
            }

            if (piPoint != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void Dispose()
        {
            piServer.Close();
        }
    }
}

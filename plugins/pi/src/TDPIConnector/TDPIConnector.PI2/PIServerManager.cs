using log4net;
using PISDK;
using System;
using System.Collections.Generic;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI2
{
    public enum ThisValType
    {
        Unknown,
        String,
        Double
    }
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

        public void DeletePoint(List<string> pointList)
        {
            foreach (var pointName in pointList)
            {
                try
                {
                    piServer.PIPoints.Remove(pointName);
                    log.Info($"{pointName} has been deleted.");
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("not exist"))
                    {
                        log.Info($"Point {pointName} does not need to be deleted because it does not exist.");
                    }
                    else
                    {
                        log.Error($"Error occured when delete point.", e);
                        throw e;
                    }
                }
            }
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

        public void CreatePoint(string pointName, ThisValType type) {
            if (!CheckPointExist(pointName))
            {
                PIPoint piPoint = piServer.PIPoints.Add(pointName, "classic", getType(type), null);
                log.Info($"{pointName} has been created.");
            }
            else {
                log.Info($"{pointName} has been exist,  not need to create.");
            }
        }

        private PointTypeConstants getType(ThisValType type) {
            if (type == ThisValType.Double)
            {
                return PointTypeConstants.pttypFloat64;
            }
            else {
                return PointTypeConstants.pttypString;
            }
        }

        public void UpdataPoint(string pointName, DateTime ts, object value)
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

using log4net;
using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using TDPIConnector.PI.Exceptions;

namespace TDPIConnector.PI
{
    public class PIServerManager : IDisposable
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly string userName;
        private readonly string password;
        private readonly string domain;
        private readonly PIServer piServer;
        public event EventHandler<PIConnection> OnConnectSuccess = delegate { };
        public event EventHandler<PIServerConnectionException> OnConnectFailure = delegate { };


        public PIServerManager(string piDataArchiveName)
        {
            this.piServer = new PIServers()[piDataArchiveName];
        }

        public PIServerManager(string piDataArchiveName, string userName, string password, string domain) : this(piDataArchiveName)
        {
            this.userName = userName;
            this.password = password;
            this.domain = domain;
        }

        public PIPointWrapper FindPIPoint(string point)
        {
            if (string.IsNullOrEmpty(point))
            {
                return null;
            }
            PIPoint piPoint = PIPoint.FindPIPoint(piServer, point);
            string pointType = piPoint.GetAttribute("pointtype").ToString();
            int pointId = Convert.ToInt32(piPoint.GetAttribute("pointid"));
            return new PIPointWrapper(piPoint, pointType, pointId);
        }

        public List<PIPointWrapper> FindPIPoints(List<string> pointNames)
        {
            List<PIPoint> piPoints = new List<PIPoint>();
            List<string> currentPIPointNames = new List<string>();
            for (int i = 0; i < pointNames.Count; i++)
            {
                currentPIPointNames.Add(pointNames[i]);
                if (i % 1000 == 0 && i > 0)
                {
                    IList<PIPoint> foundPIPoints = PIPoint.FindPIPoints(piServer, currentPIPointNames);
                    piPoints.AddRange(foundPIPoints);
                    currentPIPointNames.Clear();
                }
            }

            if (currentPIPointNames.Count > 0)
            {
                IList<PIPoint> foundPIPoints = PIPoint.FindPIPoints(piServer, currentPIPointNames);
                piPoints.AddRange(foundPIPoints);
            }
            return piPoints.Select(p => new PIPointWrapper(p)).ToList();
        }

        public PIDataPipeManager AddSignups(List<string> piPoints, IObserver<AFDataPipeEventWrapper> observerWrapper, int numberOfDataPipes)
        {
            var pointAttributeList = new List<string>()
            {
                PICommonPointAttributes.PointType, PICommonPointAttributes.Zero, PICommonPointAttributes.Span
            };

            IList<PIPoint> piPointList = PIPoint.FindPIPoints(piServer, piPoints, pointAttributeList);

            PIDataPipeManager piDataPipeManager = new PIDataPipeManager(numberOfDataPipes);
            piDataPipeManager.Subscribe(observerWrapper);
            piDataPipeManager.AddSignups(piPointList);
            return piDataPipeManager;
        }
        public List<PIPointWrapper> FindPIPoints(string pattern)
        {
            var piPoints = PIPoint.FindPIPoints(piServer, pattern);
            return piPoints.Select(p => new PIPointWrapper(p)).ToList();
        }
        public void Connect()
        {
            if (piServer == null)
            {
                throw new Exception("PI Data Archive not found.");
            }
            try
            {
                if (string.IsNullOrEmpty(userName) || string.IsNullOrEmpty(password))
                {
                    piServer.Connect();
                }
                else
                {
                    piServer.Connect(new NetworkCredential(userName, password, domain));
                }
                log.Info($"PI Data Archive Connected = {piServer.ConnectionInfo.IsConnected}");
                OnConnectSuccess(this, new PIConnection(piServer.ConnectionInfo));
            }
            catch (Exception e)
            {
                log.Error($"Error connecting to PI Data Archive.", e);
                PIServerConnectionException piServerConnectionException = new PIServerConnectionException(e);
                OnConnectFailure(this, piServerConnectionException);
                throw piServerConnectionException;
            }
        }

        public void Dispose()
        {
            piServer.Disconnect();
        }
    }
}

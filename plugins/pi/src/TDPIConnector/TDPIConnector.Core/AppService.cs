using log4net;
using Microsoft.Owin.Hosting;
using System;
using System.Text;
using System.Collections.Generic;
using TDPIConnector.Core.Monitoring;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Tasks;
using System.Threading.Tasks;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.Core
{
    public class AppService
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PIServerManager piServerManager;
        private PISystemManager piSystemManager;
        private TDEngineProxy tdEngineProxy;
        private PointModeObserver pointModeObserver;
        private ElementModeObserver elementModeObserver;
        private IDisposable webApp;
        private IMonitoringService monitoringService;
        private PointModeTask pointModeTask;
        private ElementModeTask elementModeTask;
        private StandByModeTask standByModeTask;
        private TablesCreator tablesCreator;
        private List<TDTable> piPoints;
        private Dictionary<string, AFElementWrapper> elements;
        private EventsSender eventsSender;
        private EventsSenderTask eventsSenderTask;
        private Task backfillPIPointsTask;
        private Task backfillAFElementsTask;

        public AppService()
        {
        }

        public void InitializePIConnections() {
            if (!string.IsNullOrEmpty(AppSettings.tomlConfig.PISystemName))
            {
                piSystemManager = new PISystemManager(AppSettings.tomlConfig.PISystemName);
            }
            if (!string.IsNullOrEmpty(AppSettings.tomlConfig.PIServerName))
            {
                piServerManager = new PIServerManager(AppSettings.tomlConfig.PIServerName, AppSettings.tomlConfig.PIServerUser, AppSettings.tomlConfig.PIServerPassword, AppSettings.tomlConfig.PIServerDomain);
            }
            try
            {
                //ConfigureMonitoring();

                if (piServerManager != null)
                {
                    piServerManager.Connect();
                }
                if (piSystemManager != null)
                {
                    piSystemManager.Connect();
                }
            }
            catch (Exception e)
            {
                log.Fatal("Error starting the application.Connect PI System failed!", e);
                throw e;
            }
        }

        public void InitializeTaosConnections()
        {
            if (!AppSettings.TaosXEnabled)
            {
                tdEngineProxy = TDEngineProxyBuild.NewTDEngineClient(AppSettings.TDEngineHost,
                    AppSettings.TDEnginePort,
                    AppSettings.TDEngineUsername,
                    AppSettings.TDEnginePassword,
                    AppSettings.TDEngineToken,
                    AppSettings.TDEnginePITablesPrefix
                    );
            }
            else {
                tdEngineProxy = TDEngineProxyBuild.NewTDEngineProxy(AppSettings.tomlConfig.IPCStream,
                    AppSettings.tomlConfig.SQLAPI,
                    AppSettings.TDEnginePITablesPrefix,
                    AppSettings.tomlConfig.MaxWaitLen
                    );
            }
            StaticConfig.Default
                .SetAFTreeTagName(AppSettings.tomlConfig.AFTreeTagName)
                .SetPITablesPrefix(AppSettings.TDEnginePITablesPrefix)
                .SetMaxWaitLen(AppSettings.tomlConfig.MaxWaitLen)
                .SetTDDatabase(AppSettings.tomlConfig.TDDataBase)
                .SetHttpMaxTryTimes(AppSettings.tomlConfig.HttpMaxRetryTimes);

            try
            {
                tdEngineProxy.VerifyLicenseCompability();
            }
            catch (Exception e)
            {
                log.Fatal("Error starting the application.", e);
                throw e;
            }
            ConfigureMonitoringTDEngine();
            try
            {
                tdEngineProxy.Connect();
            }
            catch (Exception e)
            {
                log.Fatal("Error starting the application. Connect TDEngine failed!", e);
                throw e;
            }
        }
        public void InitializeConnections() {
            //InitMonitoring();
            InitializePIConnections();
            InitializeTaosConnections();
            InitObserver();
        }
        private void InitMonitoring() {
            monitoringService = Container.Resolve<IMonitoringService>();
            monitoringService.Enabled = AppSettings.WebMonitoringEventsEnabled;
        }
        private void InitObserver()
        {
            this.eventsSender = new EventsSender(this.tdEngineProxy);
            this.pointModeObserver = new PointModeObserver(eventsSender);
            this.elementModeObserver = new ElementModeObserver(eventsSender);

            // eventsSender.OnPIEventReceivedListSuccess += (sender, dpEventList) => monitoringService.PublishLastPIEvent(dpEventList);
            // eventsSender.OnAFEventReceivedListSuccess += (sender, dpEventList) => monitoringService.PublishLastAFEvent(dpEventList);
            // eventsSender.OnPIEventReceivedSuccess += (sender, dpEvent) => monitoringService.PublishPIEvent(dpEvent);
            // eventsSender.OnAFEventReceivedSuccess += (sender, dpEvent) => monitoringService.PublishAFEvent(dpEvent);
            // pointModeObserver.OnPIEventReceivedFailure += (sender, ex) => monitoringService.PublishPIException(ex);
            // elementModeObserver.OnAFEventReceivedFailure += (sender, ex) => monitoringService.PublishPIException(ex);
        }
        private void ConfigureMonitoring()
        {
            piServerManager.OnConnectSuccess += (sender, piConnectionInfo) => monitoringService.PublishPIConnectionStatus(piConnectionInfo);
            piServerManager.OnConnectFailure += (sender, ex) => monitoringService.PublishPIException(ex);
        }
        private void ConfigureMonitoringTDEngine()
        {
            tdEngineProxy.OnHttpResponseReceived += (sender, httpResponse) => monitoringService.PublishTDEngineHttpResponse(httpResponse);
            tdEngineProxy.OnServerVersionReceived += (sender, version) => monitoringService.PublishTDEngineServerVersion(version);
            tdEngineProxy.OnExceptionThrown += (sender, ex) => monitoringService.PublishTDException(ex);
        }
        public async void Start()
        {
            //startWebService();
            InitializeConnections();

            this.tablesCreator = new TablesCreator(piSystemManager, piServerManager, tdEngineProxy);
            try
            {
                await tablesCreator.CreateDatabase(AppSettings.tomlConfig.TDDataBase);
                log.Info("TDengine database has been created,taosx will skip.");
            }
            catch (Exception e)
            {
                log.Fatal("Error creating TDengine database.", e);
                throw e;
            }

            if (piServerManager != null)
            {
                try
                {
                    piPoints = await tablesCreator.CreatePIPointTables(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                    log.Info($"TDengine PI Point tables ({this.piPoints.Count}) has been created.");
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating PI Point tables on TDengine.", e);
                    throw e;
                }
            }

            if (piSystemManager != null)
            {
                try
                {
                    elements = await tablesCreator.CreateAFElementTables(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                    if (elements == null)
                    {
                        log.Info($"No any AF Elements template found.");
                    }
                    else {
                        log.Info($"TDengine AF Elements tables ({this.elements.Count}) has been created.");
                    }
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating AF Element tables on TDengine.", e);
                    throw e;
                }
            }

            if ((this.piPoints != null && this.piPoints.Count > 0) || (this.elements != null && this.elements.Count > 0))
            {
                StartBackfill();
                StartDataPipe();
                this.standByModeTask = new StandByModeTask(this, piServerManager, tdEngineProxy);
                this.standByModeTask.Start();
                log.Info("Started");
            }
            else
            {
                log.Info("No PI Points or AF Elements found.");
            }
        }

        private void startWebService() {
            try
            {
                webApp = WebApp.Start<WebStartup>(url: $"{AppSettings.WebBaseUrl}:{AppSettings.WebBasePort}");
                log.Info("Web application has started.");
            }
            catch (Exception e)
            {
                log.Fatal("Error starting the web application.", e);
                throw e;
            }
        }

        public void StartBackfill()
        {
            try
            {
                if (AppSettings.tomlConfig.MaxBackfillRangeDays > 0)
                {
                    BackfillData();
                    log.Info($"Backfill started successfully.");
                }
            }
            catch (Exception e)
            {
                log.Fatal($"Error backfilling data.", e);
                throw e;
            }
        }

        private void BackfillData()
        {
            var backfillStartLimit = DateTime.UtcNow.AddDays(-AppSettings.tomlConfig.MaxBackfillRangeDays);
            this.backfillPIPointsTask = null;
            this.backfillAFElementsTask = null;
            BackfillManager backfillManager = new BackfillManager(piSystemManager, piServerManager, tdEngineProxy, tablesCreator);
            if (this.piPoints != null && this.piPoints.Count > 0)
                this.backfillPIPointsTask = backfillManager.BackfillPIPointsFromService(AppSettings.tomlConfig.TDDataBase, piPoints, backfillStartLimit);
            if (this.elements != null && this.elements.Count > 0)
                this.backfillAFElementsTask = backfillManager.BackfillAFElementsFromService(AppSettings.tomlConfig.TDDataBase, elements, backfillStartLimit);
        }

        public void StartDataPipe()
        {
            if (this.piPoints != null && this.piPoints.Count > 0)
            {
                this.pointModeTask = new PointModeTask(piServerManager, monitoringService, pointModeObserver, piPoints);
                this.pointModeTask.Start();
            }

            if (this.elements != null && this.elements.Count > 0)
            {
                this.elementModeTask = new ElementModeTask(piSystemManager, monitoringService, elementModeObserver, elements);
                this.elementModeTask.Start();
            }

            if (this.eventsSenderTask == null)
            {
                this.eventsSenderTask = new EventsSenderTask(eventsSender);
                this.eventsSenderTask.Start();
            }
        }

        public void StopDataPipe()
        {
            if (eventsSenderTask != null)
            {
                eventsSenderTask.Stop();
                eventsSenderTask = null;
            }
            if (pointModeTask != null)
            {
                pointModeTask.Stop();
                pointModeTask = null;
            }
            if (elementModeTask != null)
            {
                elementModeTask.Stop();
                elementModeTask = null;
            }
        }

        public void Stop()
        {
            if (standByModeTask != null)
            {
                standByModeTask.Stop();
            }
            StopDataPipe();
            if (piServerManager != null)
            {
                piServerManager.Dispose();
            }
            if (piSystemManager != null)
            {
                piSystemManager.Dispose();
            }
            tdEngineProxy.Dispose();
            webApp.Dispose();
        }

        public void PrintPIInfo(string pointFilter) {
            //startWebService();
            //InitMonitoring();
            InitializePIConnections();
            var scanner = new PIInfoScanner(piServerManager, piSystemManager);
            string info = scanner.GetInfo(pointFilter);
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine(info);
            log.Info(info);
            log.Info("Print PI Info finished!");
        }
        public static void GetPISDKInfo()
        {
            try
            {
                Console.WriteLine("Client SDK Version " + PISystemManager.GetPISDKInfo());
            }
            catch(Exception) {
                Console.WriteLine("PI Client SDK Not Found!");
            }
        }
    }
}

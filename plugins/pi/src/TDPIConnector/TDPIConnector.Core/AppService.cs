using log4net;
using Microsoft.Owin.Hosting;
using System;
using System.Text;
using System.Collections.Generic;
using TDPIConnector.Core.Monitoring;
using TDPIConnector.Core.ScanPiInfo;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;
using TDPIConnector.Core.Tasks;
using System.Threading;
using TDPIConnector.TDEngine.Models;
using System.Collections.Concurrent;

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
        private IMonitoringService monitoringService;
        private PointModeTask pointModeTask;
        private ElementModeTask elementModeTask;
        private StandByModeTask standByModeTask;
        private TablesCreator tablesCreator;

        public Initializer initializer { get; private set; }

        private List<TDTable> piPoints;
        private ConcurrentDictionary<string, AFElementWrapper> elements;
        private EventsSender eventsSender;
        private EventsSenderTask eventsSenderTask;
        BackfillManager backfillManager;

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
                log.Error("panic: error starting the application.Connect PI System failed!", e);
                throw new Exception($"PI System connect faied, please check config.");
            }
        }

        public void InitializeTaosConnections()
        {
            tdEngineProxy = TDEngineProxyBuild.NewTDEngineProxy(AppSettings.tomlConfig.IPCStream,
                AppSettings.tomlConfig.SQLAPI,
                AppSettings.TDEnginePITablesPrefix,
                AppSettings.tomlConfig.MaxWaitLen
                );

            StaticConfig.Default
                .SetAFTreeTagName(AppSettings.tomlConfig.AFTreeTagName)
                .SetPITablesPrefix(AppSettings.TDEnginePITablesPrefix)
                .SetMaxWaitLen(AppSettings.tomlConfig.MaxWaitLen)
                .SetTDDatabase(AppSettings.tomlConfig.TDDataBase)
                .SetHttpMaxTryTimes(AppSettings.tomlConfig.HttpMaxRetryTimes);

            try
            {
                tdEngineProxy.Connect();
            }
            catch (Exception e)
            {
                log.Fatal("panic: error starting the application. Connect TDEngine failed!", e);
                throw e;
            }
        }
        public void InitializeConnections() {
            InitializePIConnections();
            InitializeTaosConnections();
            InitObserver();
        }

        private void InitObserver()
        {
            this.eventsSender = new EventsSender(this.tdEngineProxy);
            this.pointModeObserver = new PointModeObserver(eventsSender);
            this.elementModeObserver = new ElementModeObserver(eventsSender);
        }
        public async void Start()
        {
            InitializeConnections();
            backfillManager = new BackfillManager(piSystemManager, piServerManager, tdEngineProxy, tablesCreator);
            eventsSender.SetBackfill(backfillManager);

            this.tablesCreator = new TablesCreator(piSystemManager, piServerManager, tdEngineProxy);
            this.initializer = new Initializer(ref piSystemManager, ref piServerManager, ref tdEngineProxy, ref elementModeObserver, ref eventsSender, ref backfillManager);

            if (piServerManager != null)
            {
                try
                {
                    piPoints = await tablesCreator.GetPIPointTables(AppSettings.tomlConfig.TDDataBase);
                    // piPoints = await tablesCreator.CreatePIPointTables(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                    log.Info($"TDengine PI Point tables ({this.piPoints.Count}) has been created.");
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating PI Point tables on TDengine.", e);
                    throw e;
                }
            }

            if (piSystemManager != null && !string.IsNullOrEmpty(AppSettings.tomlConfig.AFDatabaseName))
            {
                try
                {
                    await initializer.InitAFModeTask(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating AF Element tables on TDengine.", e);
                    throw e;
                }
                log.Info("InitAFModeTask finished.");
            }

            if ((this.piPoints != null && this.piPoints.Count > 0))
            {
                StartDataPipe();
                StartBackfill();
                StartTemplateObserve();
                this.standByModeTask = new StandByModeTask(this, piServerManager, tdEngineProxy);
                this.standByModeTask.Start();
                log.Info("Started");
            }
        }
        public void Wait() {
            if (AppSettings.tomlConfig.ForBackfill)
            {
                backfillManager.GetBackfill().WaitTask();
                log.Info("PI Connector finished backfill task and will quit.");
            }
            else {
                backfillManager.GetBackfill().StopAddTask();
                while (true)
                {
                    var str = Console.ReadLine();
                    if (str == "quit")
                    {
                        log.Info("TD PI Connector quit.");
                        break;
                    }
                    else
                    {
                        Thread.Sleep(5000);
                    }
                }
            }
        }
        private void StartTemplateObserve()
        {
            if (this.piSystemManager == null) {
                log.Info("Working on only point mode.");
                return;
            }
            if (AppSettings.tomlConfig.TemplateForAFElement == null ||
                AppSettings.tomlConfig.TemplateForAFElement.Count == 0)
            {
                log.Info("No ElementTemplates to watch.");
                return;
            }

            var afElementTemplateObserver = new AFElementTemplateObserver(this.piSystemManager,
               AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.TemplateForAFElement);
            afElementTemplateObserver.Observe(elementTemplateEventHandle);
        }
        public async void elementTemplateEventHandle(AFElementTemplateWrapper template)
        {
            var hasNewAttribute = await tablesCreator.CreateOrUpdateSuperTables(AppSettings.tomlConfig.TDDataBase, template);
            if (hasNewAttribute)
            {
                ReStartDataPipe();
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
            var backfillStartLimit = DateTime.UtcNow.AddMinutes(-AppSettings.tomlConfig.MaxBackfillRangeDays);
            if (this.piPoints != null && this.piPoints.Count > 0)
                backfillManager.BackfillPIPointsFromService(AppSettings.tomlConfig.TDDataBase, piPoints, backfillStartLimit);
            if (this.elements != null && this.elements.Count > 0)
                backfillManager.BackfillAFElementsFromService(AppSettings.tomlConfig.TDDataBase, elements, backfillStartLimit);
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
                this.elementModeTask = new ElementModeTask(piSystemManager, elementModeObserver, elements);
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
        }
        public void ReStartDataPipe()
        {
            log.Debug("PI Connection error detected: Stopping data pipes");
            StopDataPipe();
            piServerManager.Dispose();
            log.Debug("PI Connection error detected: Connecting to PI");
            piServerManager.Connect();
            log.Debug("PI Connection error detected: Starting data pipes");
            StartDataPipe();
            log.Debug("PI Connection error detected: Starting backfilling");
            StartBackfill();
            log.Debug("Checking PI Data Archive connection: SUCCESS");
        }
        public void PrintPIInfo(ScanMode scanMode, string filter, FilterMode filterMode) {
            //startWebService();
            //InitMonitoring();
            try {
                InitializePIConnections();
                var scanner = new PIInfoScanner(piServerManager, piSystemManager);
                string info = scanner.GetInfo(scanMode, filter, filterMode);
                Console.OutputEncoding = Encoding.UTF8;
                Console.WriteLine(info);
                log.Debug(info);
            } catch (Exception e)
            {
                Console.WriteLine(e.Message);
                log.Error(e.Message);
            }
            log.Info("Print PI Info finished!");
        }

        public void CheckConfig()
        {
            string info = "";
            try
            {
                InitializePIConnections();
            }
            catch (Exception)
            {
                info = PIConfigChecker.buildConnectFailedInfo();
                Console.OutputEncoding = Encoding.UTF8;
                Console.WriteLine(info);
                log.Info(info);
                return;
            }
            var checker = new PIConfigChecker(piServerManager, piSystemManager);
            info = checker.Check();
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine(info);
            log.Info(info);

            log.Info("Check Config finished!");
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

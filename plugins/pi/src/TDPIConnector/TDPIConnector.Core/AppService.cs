using log4net;
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
                .SetHttpMaxTryTimes(AppSettings.tomlConfig.HttpMaxRetryTimes)
                .SetBackfill(AppSettings.tomlConfig.ForBackfill)
                .SetConcurrencyCount(AppSettings.tomlConfig.BackfillConcurrencyCounts, AppSettings.tomlConfig.ConcurrencyCountsForOneTemplate);

            try
            {
                tdEngineProxy.Connect();
            }
            catch (Exception e)
            {
                log.Fatal("panic: error starting the application. Connect Agent failed!", e);
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
            eventsSender = new EventsSender(tdEngineProxy);
            pointModeObserver = new PointModeObserver(eventsSender);
            elementModeObserver = new ElementModeObserver(eventsSender);
        }
        public async void Start()
        {
            TDEngineClient.OnlyTestConnector = AppSettings.tomlConfig.OnlyTestConnector;
            // 初始化连接，并初始化 Observer
            InitializeConnections();
            // 启动 backfill 任务，一旦有 ElementBackfillTask 添加到队列，就会开始执行
            backfillManager = new BackfillManager(piSystemManager, piServerManager, tdEngineProxy, tablesCreator);
            eventsSender.SetBackfill(backfillManager);

            tablesCreator = new TablesCreator(piSystemManager, piServerManager, tdEngineProxy);
            initializer = new Initializer(ref piSystemManager, ref piServerManager, ref tdEngineProxy, ref elementModeObserver, ref eventsSender, ref backfillManager);

            if (piServerManager != null)
            {
                try
                {
                    piPoints = await tablesCreator.GetPIPointTables(AppSettings.tomlConfig.TDDataBase);
                    // piPoints = await tablesCreator.CreatePIPointTables(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                    log.Info($"TDengine PI Point tables ({piPoints.Count}) has been created.");
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating PI Point tables on TDengine.", e);
                    throw e;
                }
            }

            if (!AppSettings.tomlConfig.ForBackfill && AppSettings.tomlConfig.TemplateEventStart)
            {
                // 监听模板变化，实际上是注册数据库的事件监听函数
                StartTemplateObserve();
            }

            if (piSystemManager != null && !string.IsNullOrEmpty(AppSettings.tomlConfig.AFDatabaseName))
            {
                try
                {
                    // 启动多列模式的数据处理流程， backfill + 监听元素的属性变化
                    await initializer.InitAFModeTask(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.AFDatabaseName);
                }
                catch (Exception e)
                {
                    log.Fatal($"Error creating AF Element tables on TDengine.", e);
                    throw e;
                }
                log.Info("InitAFModeTask finished.");
            }
            
            if (piPoints != null && piPoints.Count > 0)
            {
                // 启动单列模式的数据处理流程
                StartDataPipe();
                StartBackfillPiPoints();
                standByModeTask = new StandByModeTask(this, piServerManager, tdEngineProxy);
                standByModeTask.Start();
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
            if (piSystemManager == null) {
                log.Info("Working on only point mode.");
                return;
            }
            if (AppSettings.tomlConfig.TemplateForAFElement == null ||
                AppSettings.tomlConfig.TemplateForAFElement.Count == 0)
            {
                log.Info("No ElementTemplates to watch.");
                return;
            }

            var afElementTemplateObserver = new AFElementTemplateObserver(piSystemManager, initializer,
               AppSettings.tomlConfig.AFDatabaseName, AppSettings.tomlConfig.TemplateForAFElement, tdEngineProxy);
            afElementTemplateObserver.Observe(elementTemplateEventHandle);
        }

        // 这个方法不会被调用。暂不处理模板变化事件
        public async void elementTemplateEventHandle(AFElementTemplateWrapper template)
        {
            var hasNewAttribute = await tablesCreator.CreateOrUpdateSuperTables(AppSettings.tomlConfig.TDDataBase, template);
            if (hasNewAttribute)
            {
                log.Info($"New attribute found in template {template.Name}, we can not handle this event properly now.");
                ReStartDataPipe();
            }
        }

        // 单列模式 backfill，包括 AF 单列和 Archive 单列
        public void StartBackfillPiPoints()
        {
            var config = AppSettings.tomlConfig;
            try
            {
                if (config.ForBackfill)
                {
                    DateTimeOffset startTime = config.BackfillStartTime;
                    DateTimeOffset endTime = config.BackfillEndTime;
                    if (startTime == null || endTime == null)
                    {
                        log.Info($"Backfill start time or end time is not set, will exit.");
                        return;
                    }
                    else if (startTime >= endTime)
                    {
                        log.Info($"Backfill start time: {startTime}, end time: {endTime}, will exit.");
                        return;
                    }
                    else
                    {
                        log.Info($"Backfill start time: {startTime}, end time: {endTime}");
                        BackfillPIPoints(startTime.DateTime, endTime.DateTime);
                        log.Info($"Backfill started successfully.");
                    }

                }
                else { 
                    if (config.MaxBackfillRangeDays > 0)
                    {
                        log.Info($"Backfill range is set to {config.MaxBackfillRangeDays} minutes.");
                        DateTime startTime = DateTime.UtcNow.AddMinutes(-config.MaxBackfillRangeDays);
                        DateTime endTime = DateTime.UtcNow;
                        log.Info($"Backfill start time: {startTime}, end time: {endTime}");
                        BackfillPIPoints(startTime, endTime);
                        log.Info($"Backfill started successfully.");
                    }
                    else
                    {
                        log.Info($"Backfill is not enabled.");
                    }
                
                }
            }
            catch (Exception e)
            {
                log.Fatal($"Error backfilling data.", e);
                throw e;
            }
        }

        private void BackfillPIPoints(DateTime backfillStartTime, DateTime backfillEndTime)
        {
                backfillManager.BackfillPIPointsFromService(AppSettings.tomlConfig.TDDataBase, piPoints, backfillStartTime, backfillEndTime);
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
            StartBackfillPiPoints();
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
            string info;
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

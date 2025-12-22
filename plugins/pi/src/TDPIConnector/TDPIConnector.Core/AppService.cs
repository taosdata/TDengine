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
using System.Threading.Tasks;

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
        private Task pointsBackfillTask;
        private readonly ManualResetEventSlim _serviceStop = new ManualResetEventSlim(false);
        // 防止在服务模式下重复触发 Stop
        private int _serviceStopping = 0; // 0:not stopping, 1:stopping
        public AppService()
        {
        }

        public void InitializePIConnections()
        {
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
                throw;
            }
        }
        public void InitializeConnections()
        {
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
        public async Task Start()
        {
            TDEngineClient.OnlyTestConnector = AppSettings.tomlConfig.OnlyTestConnector;
            // 初始化连接，并初始化 Observer
            InitializeConnections();
            // 启动 backfill 任务，一旦有 ElementBackfillTask 添加到队列，就会开始执行
            tablesCreator = new TablesCreator(piSystemManager, piServerManager, tdEngineProxy);
            backfillManager = new BackfillManager(piSystemManager, piServerManager, tdEngineProxy, tablesCreator);
            // 关联 backfill 到已存在的 EventsSender
            eventsSender.SetBackfill(backfillManager);
            // 初始化器
            initializer = new Initializer(ref piSystemManager, ref piServerManager, ref tdEngineProxy, ref elementModeObserver, ref eventsSender, ref backfillManager);

            if (piServerManager != null)
            {
                try
                {
                    piPoints = await tablesCreator.GetPIPointTables(AppSettings.tomlConfig.TDDataBase);
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
                if (!AppSettings.tomlConfig.ForBackfill)
                {
                    StartDataPipe();
                    standByModeTask = new StandByModeTask(this, piServerManager, tdEngineProxy);
                    standByModeTask.Start();
                }

                // 启动点位回填任务
                StartBackfillPiPoints();
                log.Info("Started");
            }
        }
        public void Wait()
        {
            // windows service 模式下
            if (!Environment.UserInteractive)
            {
                log.Info("Running as Windows Service, blocking until Stop is called.");
                _serviceStop.Wait();
                return;
            }

            if (AppSettings.tomlConfig.ForBackfill)
            {
                // 确保不再有后台任务阻塞退出
                if (standByModeTask != null)
                {
                    log.Info("Stopping StandByModeTask...");
                    standByModeTask.Stop();
                    standByModeTask = null;
                    log.Info("StandByModeTask stopped.");
                }
                log.Info("Stopping data pipes...");
                StopDataPipe();
                log.Info("Data pipes stopped.");

                // 先等待点位回填任务完成
                if (pointsBackfillTask != null)
                {
                    try
                    {
                        log.Info("Waiting pointsBackfillTask to complete...");
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        pointsBackfillTask.Wait();
                        sw.Stop();
                        log.Info($"pointsBackfillTask completed in {sw.ElapsedMilliseconds} ms.");
                    }
                    catch (AggregateException ae)
                    {
                        log.Error("pointsBackfillTask.Wait() error", ae);
                    }
                }
                else
                {
                    log.Warn("pointsBackfillTask is null, skipping wait.");
                }

                log.Info("Stopping adding new backfill tasks.");
                backfillManager.GetBackfill().StopAddTask();
                log.Info("StopAddTask invoked.");

                log.Info("Waiting backfill tasks to finish.");
                var sw2 = System.Diagnostics.Stopwatch.StartNew();
                backfillManager.GetBackfill().WaitTask();
                sw2.Stop();
                log.Info($"WaitTask returned after {sw2.ElapsedMilliseconds} ms.");

                log.Info("PI Connector finished backfill task and will quit.");
                return;
            }
            else
            {
                log.Info("Wait() enter non-backfill interactive branch");
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
                        log.Info($"Console input: '{str}', sleeping 5 seconds.");
                        Thread.Sleep(5000);
                    }
                }
            }
        }
        private void StartTemplateObserve()
        {
            if (piSystemManager == null)
            {
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
            try
            {
                var hasNewAttribute = await tablesCreator.CreateOrUpdateSuperTables(AppSettings.tomlConfig.TDDataBase, template);
                if (hasNewAttribute)
                {
                    log.Info($"New attribute found in template {template.Name}, we can not handle this event properly now.");
                    ReStartDataPipe();
                }
            }
            catch (Exception e)
            {
                log.Error($"Error handling template event for template {template.Name}.", e);
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
                    if (startTime == default || endTime == default)
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
                else
                {
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
            pointsBackfillTask =
                backfillManager.BackfillPIPointsFromService(AppSettings.tomlConfig.TDDataBase, piPoints, backfillStartTime, backfillEndTime);

            // 服务模式下，PI 点位回填完成后停止服务以退出
            if (!Environment.UserInteractive && AppSettings.tomlConfig.ForBackfill)
            {
                pointsBackfillTask.ContinueWith((t) =>
                {
                    if (System.Threading.Interlocked.Exchange(ref _serviceStopping, 1) == 1)
                    {
                        log.Warn("Service is already stopping, skipping repeated Stop call.");
                        return;
                    }

                    if (t.IsFaulted)
                    {
                        log.Error("Points backfill task failed in service mode.", t.Exception);
                    }
                    else
                    {
                        log.Info("Points backfill task completed successfully in service mode, stopping the service.");
                    }

                    try
                    {
                        if (backfillManager != null)
                        {
                            log.Info("Service mode: StopAddTask and WaitTask before Stop.");
                            backfillManager.GetBackfill().StopAddTask();
                            backfillManager.GetBackfill().WaitTask();
                        }
                        else
                        {
                            log.Warn("Service mode: backfillManager is null, skipping StopAddTask and WaitTask.");
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Warn("Service mode: WaitTask error, continue to Stop.", ex);
                    }

                    log.Info("Service mode: Backfill finished, stopping the service.");
                    Stop(); // 触发 _serviceStop.Set()，使 Wait 方法退出
                }, TaskScheduler.Default);
            }
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
            System.Threading.Interlocked.Exchange(ref _serviceStopping, 1);
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
            if (tdEngineProxy != null)
            {
                tdEngineProxy.Dispose();
            }
            // 通知 Wait 方法退出
            _serviceStop.Set();
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
        public void PrintPIInfo(ScanMode scanMode, string filter, FilterMode filterMode)
        {
            //startWebService();
            //InitMonitoring();
            try
            {
                InitializePIConnections();
                var scanner = new PIInfoScanner(piServerManager, piSystemManager);
                string info = scanner.GetInfo(scanMode, filter, filterMode);
                Console.OutputEncoding = Encoding.UTF8;
                Console.WriteLine(info);
                log.Debug(info);
            }
            catch (Exception e)
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
            catch (Exception)
            {
                Console.WriteLine("PI Client SDK Not Found!");
            }
        }
    }
}

using log4net;
using TDPIConnector.PI2;
using TDPIConnector.Core;
using TDPIConnector.TDEngine;
using System.Collections.Generic;
using TDPIConnector.TDEngine.Models;
using System.Threading.Tasks;
using System;

namespace PISimulator
{
    class PointsDropper
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private PIServerManager piServerManager;
        private TDEngineProxy tdEngineProxy;

        public PointsDropper(string piServerName)
        {
            piServerManager = new PIServerManager(piServerName);
        }
        public void Start() {
            piServerManager.Connect();
            piServerManager.DeletePoint(AppSettings.tomlConfig.PointList);

            if (AppSettings.TDEngineHost != "") {
                try
                {
                    TDengineConnect();
                    DropTDengineTables(AppSettings.tomlConfig.TDDataBase, AppSettings.tomlConfig.PointList);
                }
                catch (Exception e) {
                    log.Info($"TDengine Connect failed! Delete TDengine tables skip.{e.Message}");
                }
            }

        }
        public void DropTDengineTables(string db, List<string> points) {
            log.Info($"Dropping {points.Count} PI Points tables from db:{db}.");
            List<Task<TDEngineResponse>> tasks = new List<Task<TDEngineResponse>>();
            foreach (var piPoint in points)
            {
                tasks.Add(tdEngineProxy.DropTableForPIPoint(AppSettings.tomlConfig.TDDataBase, piPoint));
                log.Info($"table {piPoint} dropped.");
            }
            log.Info("Please wait for the task to be completed...");
            Task.WhenAll(tasks).Wait();
        }
        public void TDengineConnect() {
            TDengineInit();
            tdEngineProxy.Connect();
        }
        public void TDengineInit() {
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
            else
            {
                tdEngineProxy = TDEngineProxyBuild.NewTDEngineProxy(AppSettings.tomlConfig.IPCStream,
                    AppSettings.tomlConfig.SQLAPI,
                    AppSettings.TDEnginePITablesPrefix,
                    AppSettings.tomlConfig.MaxWaitLen
                    );
            }
        }
    }
}

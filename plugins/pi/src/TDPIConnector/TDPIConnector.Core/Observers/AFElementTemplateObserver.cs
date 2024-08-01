using System;
using System.Collections.Generic;
using log4net;
using OSIsoft.AF;
using TDPIConnector.PI;
using TDPIConnector.TDEngine;

namespace TDPIConnector.Core
{
    public class AFElementTemplateEvent { }

    public class AFElementTemplateObserver
    {
        private readonly AFDatabase db;
        readonly PISystemManager piSystemManager;
        private readonly TDEngineProxy tdEngineProxy;
        private readonly bool SyncDeleteElement;
        private readonly bool SyncAddElement;
        Initializer initializer;
        System.Timers.Timer refreshTimer = new System.Timers.Timer(10 * 1000); // 10 秒
        private static readonly ILog log = LogManager.GetLogger(
            System.Reflection.MethodBase.GetCurrentMethod().DeclaringType
        );

        HashSet<string> observeSet;
        Action<AFElementTemplateWrapper> elementTemplateEventHandle;

        public AFElementTemplateObserver(
            PISystemManager piSystemManager,
            Initializer initializer,
            string afDatabaseName,
            List<string> ElementTemplates2,
            TDEngineProxy tdEngineProxy
        )
        {
            this.piSystemManager = piSystemManager;
            this.initializer = initializer;
            this.tdEngineProxy = tdEngineProxy;
            db = piSystemManager.GetAFDatabase(afDatabaseName);
            if (db == null)
            {
                throw new Exception("AF Database not found.");
            }
            observeSet = new HashSet<string>(ElementTemplates2);
            SyncDeleteElement = AppSettings.tomlConfig.SyncDeleteElement;
            SyncAddElement = AppSettings.tomlConfig.SyncAddElement;
        }

        public void Observe(Action<AFElementTemplateWrapper> elementTemplateEventHandle)
        {
            this.elementTemplateEventHandle = elementTemplateEventHandle;
            EventHandler<AFChangedEventArgs> changedEH = new EventHandler<AFChangedEventArgs>(
                OnChanged
            );
            db.Changed += changedEH;

            System.Timers.ElapsedEventHandler elapsedEH = new System.Timers.ElapsedEventHandler(
                OnTemplateElapsed
            );
            refreshTimer.Elapsed += elapsedEH;
            refreshTimer.Start();
        }

        internal void OnChanged(object sender, AFChangedEventArgs e)
        {
            log.Info($"AFChangedEvent:{e.Action}:{e.ID},Identity={e.Identity}");
            // e.Identity 表示事件的对象的类型
            if (e.Identity == AFIdentity.ElementTemplate)
            {
                // 暂不处理模板本身变化的事件， 只在日志中记录事件
                AFElementTemplateWrapper template = piSystemManager.GetElementsByTemplateID(e.ID);
                log.Info(
                    $"AFChangedEvent:{e.ID},Template={template.Name},Action={e.Action}.Ignored"
                );
                return;
                //if (observeSet.Contains(template.Name))
                //{
                //    elementTemplateEventHandle(template);
                //}
            }
            else if (e.Identity == AFIdentity.Element)
            {
                // 模板元素变化事件
                AFElementWrapper element = piSystemManager.GetElementsById(e.ID);
                if (element == null)
                {
                    log.Info($"AFChangedEvent:{e.Action}:{e.ID},Element not actually exists.Ignored");
                    return;
                }
                if (e.Action == AFChangeAction.SubObjectAdd)
                {
                    if (!SyncAddElement)
                    {
                        log.Info($"AFChangedEvent:{e.Action}:{e.ID},{element.Name}.Ignored");
                        return;
                    }
                    // 添加新元素, 此时还没有 CheckIn
                    initializer.AddOrRefreshElementToTaskAsync(element).Wait();
                    log.Info($"AFChangedEvent:{e.Action}:{e.ID},{element.Name}.Done");
                    return;
                }
                else if (e.Action == AFChangeAction.SubObjectRefresh)
                {
                    // 刷新元素, 很多情况都会触发
                    initializer.AddOrRefreshElementToTaskAsync(element).Wait();
                    log.Info($"AFChangedEvent:{e.Action}:{e.ID}.{element.Name}.Done");
                    return;
                }
                else if (e.Action == AFChangeAction.SubObjectChange)
                {
                    // 修改元素, CheckIn 会触发此事件
                    initializer.AddOrRefreshElementToTaskAsync(element).Wait();
                    log.Info($"AFChangedEvent:{e.Action}:{e.ID}.{element.Name}.Done");
                    return;
                }
                else if (e.Action == AFChangeAction.SubObjectRemove)
                {
                    if (!SyncDeleteElement)
                    {
                        log.Info($"AFChangedEvent:{e.Action}:{e.ID},{element.Name}.Ignored");
                        return;
                    }
                    // 删除元素
                    tdEngineProxy.DropElement(e.ID.ToString());
                    log.Info($"AFChangedEvent:{e.Action}:{e.ID},{element.Name}.Done");
                    return;
                }
            }
            log.Info($"AFChangedEvent:{e.Action}:{e.ID},Identity={e.Identity}.Ingored");
        }

        internal void OnTemplateElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (db)
            {
                db.Refresh();
            }
            refreshTimer.Start();
        }
    }
}

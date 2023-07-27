using System;
using System.Collections.Generic;
using OSIsoft.AF;
using log4net;

namespace TDPIConnector.PI
{
    public class AFElementTemplateEvent
    {
    }
    public class AFElementTemplateObserver
    {
        private readonly AFDatabase db;
        readonly PISystemManager piSystemManager;
        System.Timers.Timer refreshTimer = new System.Timers.Timer(10 * 1000); // every 60 seconds
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        HashSet<string> observeSet;
        Action<AFElementTemplateWrapper> elementTemplateEventHandle;

        public AFElementTemplateObserver(PISystemManager piSystemManager, string afDatabaseName, List<string> ElementTemplates2)
        {
            this.piSystemManager = piSystemManager;
            db = piSystemManager.GetAFDatabase(afDatabaseName);
            if (db == null)
            {
                throw new Exception("AF Database not found.");
            }
            observeSet = new HashSet<string>(ElementTemplates2);
        }

        public void Observe(Action<AFElementTemplateWrapper> elementTemplateEventHandle)
        {
            this.elementTemplateEventHandle = elementTemplateEventHandle;
            EventHandler<AFChangedEventArgs> changedEH = new EventHandler<AFChangedEventArgs>(OnChanged);
            db.Changed += changedEH;

            System.Timers.ElapsedEventHandler elapsedEH = new System.Timers.ElapsedEventHandler(OnTemplateElapsed);
            refreshTimer.Elapsed += elapsedEH;
            refreshTimer.Start();
        }

        internal void OnChanged(object sender, AFChangedEventArgs e)
        {
            if (e.Identity == AFIdentity.ElementTemplate)
            {
                var template = piSystemManager.GetElementsByTemplateID(e.ID);
                if (observeSet.Contains(template.Name))
                {
                    log.Info($"Object Changed: {e.Action}  {e.Identity} sub: {e.IsSubObjectEvent}");
                    elementTemplateEventHandle(template);
                }
            }
            else if (e.Identity == AFIdentity.Element)
            {
                // var element = AFElement.FindElement(piSystem, e.ID);
                // log.Info($"Object Changed: {e.Action}  {e.Identity} sub: {e.IsSubObjectEvent}");
            }
            else
            {
                log.Info($"Object Changed: {e.Action}  {e.Identity}");
            }
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
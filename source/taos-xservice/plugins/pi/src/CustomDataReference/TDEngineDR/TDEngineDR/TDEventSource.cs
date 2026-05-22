using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.Time;
using System;
using System.Collections.Generic;

namespace TDEngineDR
{
    public class TDEventSource : AFEventSource
    {
        Dictionary<AFAttribute, AFTime> _lastTimes = new Dictionary<AFAttribute, AFTime>();

        AFTime _startTime;

        public TDEventSource()
        {
            _startTime = new AFTime("*-1s");
        }

        protected override void Dispose(bool disposing)
        {
            _lastTimes = null;
        }

        protected override bool GetEvents()
        {
            bool hasNewEvents = false;
            AFTime evalTime = AFTime.Now;

            IEnumerable<AFAttribute> signupList = base.Signups;

            foreach (AFAttribute att in signupList)
            {
                if (ReferenceEquals(att, null)) continue;

                try
                {
                    if (!_lastTimes.ContainsKey(att))
                    {
                        _lastTimes.Add(att, this._startTime);
                    }

                    AFTimeRange timeRange = new AFTimeRange(_lastTimes[att], evalTime);
                    AFValues vals = att.GetValues(timeRange, 0, att.DefaultUOM);

                    foreach (AFValue val in vals)
                    {
                        if (val.Timestamp > _lastTimes[att])
                        {
                            AFDataPipeEvent ev = new AFDataPipeEvent(AFDataPipeAction.Add, val);
                            base.PublishEvent(att, ev);
                            _lastTimes[att] = val.Timestamp;
                            hasNewEvents = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SimpleLogger.Instance.Error($"TDEventSource GetEvents error for {att.Name}: {ex.Message}");
                }
            }

            return hasNewEvents;
        }
    }
}

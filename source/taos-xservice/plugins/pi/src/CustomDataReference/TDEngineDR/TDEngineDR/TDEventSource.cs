using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.Time;
using System.Collections.Generic;

namespace TDEngineDR
{
    class TDEventSource : AFEventSource
    {
        Dictionary<AFAttribute, AFTime> _lastTimes = new Dictionary<AFAttribute, AFTime>();


        AFTime _startTime;

        public TDEventSource()
        {
            _startTime = new AFTime("*");
        }

        protected override void Dispose(bool disposing)
        {
            _lastTimes = null;
        }

        protected override bool GetEvents()
        {
            AFTime evalTime = AFTime.Now;

            IEnumerable<AFAttribute> signupList = base.Signups;

            foreach (AFAttribute att in signupList)
            {
                if (!ReferenceEquals(att, null))
                {
                    if (!_lastTimes.ContainsKey(att))
                    {
                        _lastTimes.Add(att, this._startTime);
                    }

                    AFTimeRange timeRange = new AFTimeRange(_lastTimes[att], evalTime);

                    AFValues vals = att.GetValues(timeRange, 0, att.DefaultUOM);

                    AFTime lastTime = _lastTimes[att];

                    foreach (AFValue val in vals)
                    {
                        if (val.Timestamp > lastTime)
                        {
                            AFDataPipeEvent ev = new AFDataPipeEvent(AFDataPipeAction.Add, val);
                            base.PublishEvent(att, ev);
                        }


                        if (val.Timestamp > lastTime)
                        {
                            _lastTimes[att] = val.Timestamp;
                        }
                    }
                }
            }
            return false;
        }
    }
}

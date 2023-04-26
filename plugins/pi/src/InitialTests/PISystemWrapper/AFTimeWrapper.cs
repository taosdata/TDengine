using OSIsoft.AF.Time;
using System;

namespace PISystemWrapper
{
    public class AFTimeWrapper
    {
        internal AFTime AFSDKObject { get; private set; }
        internal AFTimeWrapper(AFTime afTime)
        {
            this.AFSDKObject = afTime;
        }

        public AFTimeWrapper(DateTime dt)
        {
            this.AFSDKObject = new AFTime(dt);
        }

        public DateTime UtcTime
        {
            get
            {
                return this.AFSDKObject.UtcTime;
            }
        }

        public DateTime LocalTime
        {
            get
            {
                return this.AFSDKObject.LocalTime;
            }
        }
    }
}
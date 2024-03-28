using OSIsoft.AF.Time;
using System;

namespace TDPIConnector.PI
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

        public string FormatUtcTime() {
            return this.AFSDKObject.LocalTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }

        public static bool operator ==(AFTimeWrapper lhs, AFTimeWrapper rhs) {
            return lhs.AFSDKObject == rhs.AFSDKObject;
        }

        public static bool operator !=(AFTimeWrapper lhs, AFTimeWrapper rhs) {
            return lhs.AFSDKObject != rhs.AFSDKObject;
        }

        public static bool operator <(AFTimeWrapper lhs, AFTimeWrapper rhs)
        {
            return lhs.AFSDKObject < rhs.AFSDKObject;
        }

        public static bool operator >(AFTimeWrapper lhs, AFTimeWrapper rhs)
        {
            return lhs.AFSDKObject > rhs.AFSDKObject;
        }

        public static bool operator <=(AFTimeWrapper lhs, AFTimeWrapper rhs)
        {
            return lhs.AFSDKObject <= rhs.AFSDKObject;
        }

        public static bool operator >=(AFTimeWrapper lhs, AFTimeWrapper rhs)
        {
            return lhs.AFSDKObject >= rhs.AFSDKObject;
        }
    }
}
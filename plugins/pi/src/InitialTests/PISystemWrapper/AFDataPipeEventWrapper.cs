using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.Time;
using System;

namespace PISystemWrapper
{
    public class AFDataPipeEventWrapper
    {
        internal AFDataPipeEventWrapper(AFDataPipeEvent dataPipeEvent)
        {
            this.AFSDKObject = dataPipeEvent.Value;
        }

        public AFDataPipeEventWrapper()
        {
            this.AFSDKObject = new AFValue();
        }


        public virtual AFValueWrapper Value
        {
            get
            {
                return new AFValueWrapper(this.AFSDKObject);
            }
        }
        public virtual PIPointWrapper Point
        {
            get
            {
                return new PIPointWrapper(this.AFSDKObject.PIPoint);
            }
        }
        public virtual AFValue AFSDKObject { get; }

        //need to implement?
        //public TypeCode ValueTypeCode
        //{
        //    get
        //    {
        //        return this.AFSDKObject.ValueTypeCode;
        //    }
        //}


    }
}

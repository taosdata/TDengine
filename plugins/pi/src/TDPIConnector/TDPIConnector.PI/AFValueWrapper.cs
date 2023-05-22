using OSIsoft.AF.Asset;
using OSIsoft.AF.Time;
using System;

namespace TDPIConnector.PI
{
    public class AFValueWrapper
    {
        internal AFValueWrapper(AFValue value)
        {
            this.AFSDKObject = value;
        }

        public bool IsAFEnumerationValue()
        {
            return this.AFSDKObject.Value is AFEnumerationValue;
        }

        public AFEnumerationValueWrapper GetEnumerationValue()
        {
            AFEnumerationValue enumValue = (AFEnumerationValue)this.Value;
            return new AFEnumerationValueWrapper(enumValue);
        }

        public AFValueWrapper()
        {
            this.AFSDKObject = new AFValue();
        }

        public AFValueWrapper(double val, DateTime dt)
        {
            this.AFSDKObject = new AFValue(val, dt);
        }

        public virtual AFAttributeWrapper Attribute
        {
            get
            {
                return new AFAttributeWrapper(this.AFSDKObject.Attribute);
            }
            set
            {
                this.AFSDKObject.Attribute = value.AFSDKObject;
            }
        }
        public virtual AFTimeWrapper Timestamp
        {
            get
            {
                return new AFTimeWrapper(this.AFSDKObject.Timestamp);
            }
            set
            {
                this.AFSDKObject.Timestamp = new AFTime(value.LocalTime);
            }
        }

        public virtual object Value
        {
            get
            {
                return this.AFSDKObject.Value;
            }
            set
            {
                this.AFSDKObject.Value = value;
            }
        }

        public virtual AFValue AFSDKObject { get; }
        public TypeCode ValueTypeCode
        {
            get
            {
                return this.AFSDKObject.ValueTypeCode;
            }
        }

        public PIPointWrapper PIPoint
        {
            get
            {
                return this.AFSDKObject.PIPoint != null ? new PIPointWrapper(this.AFSDKObject.PIPoint) : null;
            }
        }

        public virtual int ValueAsInt32()
        {
            return this.AFSDKObject.ValueAsInt32();
        }

        public virtual double ValueAsDouble()
        {
            return this.AFSDKObject.ValueAsDouble();
        }
        public bool OnMaxTime()
        {
            return Timestamp.AFSDKObject == AFTime.MaxValue;
        }
    }
}

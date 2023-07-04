using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.EventFrame;
using System;
using System.Collections.Generic;
using System.Linq;
using log4net;

namespace TDPIConnector.PI
{
    public class AFAttributeWrapper
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        internal AFAttribute AFSDKObject { get; private set; }
        internal AFAttributeWrapper(AFAttribute attribute)
        {
            this.AFSDKObject = attribute;
        }

        public AFAttributeWrapper()
        {

        }

        public virtual string ID
        {
            get
            {
                return this.AFSDKObject.ID.ToString();
            }
        }

        public virtual IEnumerable<string> Categories
        {
            get
            {
                return this.AFSDKObject.Categories.Select(c => c.Name);
            }
        }


        public virtual PIPointWrapper PIPoint
        {
            get
            {
                try
                {
                    if (AFSDKObject.PIPoint == null)
                    {
                        return null;
                    }
                    return new PIPointWrapper(this.AFSDKObject.PIPoint);
                }
                catch (Exception e)
                {
                    log.Warn($"Not Found Point, {e.Message}");
                    return null;
                }
            }
        }

        internal AFAttributeWrapper GetElementAttribute()
        {
            if (AFSDKObject == null)
            {
                return null;
            }      

            if (AFSDKObject.Element == null)
            {
                return this;
            }
            if (AFSDKObject.Element is AFEventFrame)
            {
                AFEventFrame ef = (AFEventFrame)AFSDKObject.Element;
                AFElement element = ef.PrimaryReferencedElement;

                if (string.IsNullOrEmpty(this.AFSDKObject.ConfigString))
                {
                    return null;
                }
                AFAttribute elementAttribute = GetAttributeFromEfAttributeConfigString();
                if (elementAttribute == null)
                {
                    return null;
                }
                return new AFAttributeWrapper(elementAttribute);
            }

            if (AFSDKObject.Element is AFElement)
            {
                return this;
            }

            return this;
        }

        private AFAttribute GetAttributeFromEfAttributeConfigString()
        {
            string path = this.AFSDKObject.ConfigString.Split(';')[0];
            path = path.Replace("%Attribute%", this.AFSDKObject.Name);
            return AFObject.FindObject(path, this.AFSDKObject) as AFAttribute;
        }

        public virtual AFElementWrapper Element
        {
            get
            {
                if (AFSDKObject.Element == null)
                {
                    return null;
                }
                return new AFElementWrapper((AFElement)this.AFSDKObject.Element);
            }
        }


        public virtual string Name
        {
            get
            {

                return this.AFSDKObject.Name;
            }
        }

        public bool IsPIPointDataReference
        {
            get
            {

                return this.AFSDKObject.DataReferencePlugIn != null && this.AFSDKObject.DataReferencePlugIn.Name == "PI Point";
            }
        }

        public bool IsConfigurationItem
        {
            get
            {
                return this.AFSDKObject.IsConfigurationItem;
            }
        }

        public string Uom
        {
            get
            {

                return this.AFSDKObject.DefaultUOM != null ? this.AFSDKObject.DefaultUOM.Abbreviation : null;
            }
        }

        public Type Type
        {
            get
            {
                return this.AFSDKObject.Type;
            }
        }

        public string ConfigurationItem
        {
            get
            {
                return this.AFSDKObject.IsConfigurationItem ? this.AFSDKObject.GetValue().Value.ToString() : string.Empty;
            }
        }

        public virtual string GetPath()
        {
            return this.AFSDKObject.GetPath();
        }

        public override bool Equals(object obj)
        {
            if (obj is AFAttribute)
            {
                AFAttribute attribute = (AFAttribute)obj;
                return this.AFSDKObject.Equals(attribute);
            }
            else if (obj is AFAttributeWrapper)
            {
                AFAttributeWrapper attribute = (AFAttributeWrapper)obj;
                return this.AFSDKObject.Equals(attribute.AFSDKObject);
            }
            else if (obj is AFObject)
            {
                AFObject afObject = (AFObject)obj;
                return this.AFSDKObject.Equals(afObject);
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return this.AFSDKObject.GetHashCode();
        }

        public virtual AFValueWrapper GetValue()
        {
            AFValue value = AFSDKObject.GetValue();
            if (value == null)
            {
                return null;
            }
            return new AFValueWrapper(value);
        }
    }
}

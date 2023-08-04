using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TDPIConnector.PI
{
    public class AFAttributeTemplateWrapper
    {
        internal AFAttributeTemplate AFSDKObject { get; private set; }
        internal AFAttributeTemplateWrapper(AFAttributeTemplate attribute)
        {
            this.AFSDKObject = attribute;
        }

        public AFAttributeTemplateWrapper()
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

        public Type Type
        {
            get
            {
                return this.AFSDKObject.Type;
            }
        }

        public string Uom
        {
            get
            {
                return this.AFSDKObject.DefaultUOM != null ? this.AFSDKObject.DefaultUOM.Abbreviation : string.Empty;
            }
        }

        public string ConfigurationItem
        {
            get
            {
                return this.AFSDKObject.IsConfigurationItem ? (string)this.AFSDKObject.GetValue(null) : string.Empty;
            }
        }
        public string DataReference
        {
            get
            {
                if (this.AFSDKObject.DataReference == null)
                {
                    return "";
                }
                return this.AFSDKObject.DataReference.Name;
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
    }
}

using OSIsoft.AF.Asset;
using System;

namespace TDPIConnector.PI
{
    public class AFElementWrapper
    {
        internal AFElement AFSDKObject { get; private set; }
        internal AFElementWrapper(AFElement element)
        {
            this.AFSDKObject = element;
        }
        public AFElementWrapper()
        {

        }
        public AFElementTemplateWrapper Template
        {
            get
            {
                return new AFElementTemplateWrapper(this.AFSDKObject.Template);
            }
        }
        public virtual string Name
        {
            get
            {
                return this.AFSDKObject.Name;
            }
        }
        public virtual Guid ID
        {
            get
            {
                return this.AFSDKObject.ID;
            }
        }
        public AFAttributesWrapper Attributes
        {
            get
            {
                return new AFAttributesWrapper(this.AFSDKObject.Attributes);
            }
        }

        public AFDatabaseWrapper Database
        {
            get
            {
                return new AFDatabaseWrapper(this.AFSDKObject.Database);
            }
        }
        public virtual string GetPath()
        {
            return this.AFSDKObject.GetPath();
        }
        public bool HasInvalidAttr()
        {
            foreach (var att in this.AFSDKObject.Attributes)
            {
                try
                {
                    if (att.PIPoint == null)
                        ;// do noting, just for exception
                }
                catch (Exception e)
                {
                      return true;
                }
            }
            return false;
        }
    }
}

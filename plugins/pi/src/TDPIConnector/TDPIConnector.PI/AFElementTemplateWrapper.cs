using OSIsoft.AF.Asset;

namespace TDPIConnector.PI
{
    public class AFElementTemplateWrapper
    {
        internal AFElementTemplate AFSDKObject { get; private set; }
        internal AFElementTemplateWrapper(AFElementTemplate elementTemplate)
        {
            this.AFSDKObject = elementTemplate;
        }

        public AFElementTemplateWrapper()
        {

        }

        public virtual string Name
        {
            get
            {
                return this.AFSDKObject.Name;
            }
        }
        public AFElementTemplateWrapper BaseTemplate()
        {
            if (this.AFSDKObject.BaseTemplate != null)
            {
                return new AFElementTemplateWrapper(this.AFSDKObject.BaseTemplate);
            }
            else
            {
                return null;
            }
        }

        public AFAttributeTemplatesWrapper AttributeTemplates
        {
            get
            {
                var ret = new AFAttributeTemplatesWrapper(this.AFSDKObject.AttributeTemplates);
                var baseTem = this.AFSDKObject.BaseTemplate;
                while (baseTem != null) {
                    ret.AppendAFAttributeTemplates(baseTem.AttributeTemplates);
                    baseTem = baseTem.BaseTemplate;
                }
                return ret;
            }
        }

        public virtual string GetPath()
        {
            return this.AFSDKObject.GetPath();
        }
    }
}

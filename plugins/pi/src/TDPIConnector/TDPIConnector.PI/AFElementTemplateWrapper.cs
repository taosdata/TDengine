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

        public AFAttributeTemplatesWrapper AttributeTemplates
        {
            get
            {
                return new AFAttributeTemplatesWrapper(this.AFSDKObject.AttributeTemplates);
            }
        }

        public virtual string GetPath()
        {
            return this.AFSDKObject.GetPath();
        }
    }
}

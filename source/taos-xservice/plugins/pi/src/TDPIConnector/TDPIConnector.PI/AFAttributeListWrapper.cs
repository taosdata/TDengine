using OSIsoft.AF.Asset;
using System.Collections.Generic;
using System.Linq;

namespace TDPIConnector.PI
{
    public class AFAttributeListWrapper : List<AFAttributeWrapper>
    {
        public AFAttributeList AFSDKObject { get; }

        internal AFAttributeListWrapper(AFAttributes attributes)
        {
            this.AFSDKObject = new AFAttributeList(attributes);
            foreach (AFAttribute attribute in attributes)
            {
                this.Add(new AFAttributeWrapper(attribute));
            }
        }

        public AFAttributeListWrapper()
        {
            this.AFSDKObject = new AFAttributeList();
        }

        public void AddRange(AFAttributesWrapper attributes)
        {
            foreach (var attribute in attributes)
            {
                this.AFSDKObject.Add(attribute.AFSDKObject);
                this.Add(attribute);
            }
        }

        public AFAttributeWrapper this[string attributeName]
        {
            get
            {
                return this.Where(a => a.Name == attributeName).SingleOrDefault();
            }
        }
    }
}
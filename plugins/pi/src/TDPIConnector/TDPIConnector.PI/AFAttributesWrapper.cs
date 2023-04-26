using OSIsoft.AF.Asset;
using System.Collections.Generic;
using System.Linq;

namespace TDPIConnector.PI
{
    public class AFAttributesWrapper : List<AFAttributeWrapper>
    {
        public AFAttributes AFSDKObject { get; }

        internal AFAttributesWrapper(AFAttributes attributes)
        {
            this.AFSDKObject = attributes;
            foreach (AFAttribute attribute in attributes)
            {
                this.Add(new AFAttributeWrapper(attribute));
            }
        }

        public AFAttributesWrapper()
        {
            
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
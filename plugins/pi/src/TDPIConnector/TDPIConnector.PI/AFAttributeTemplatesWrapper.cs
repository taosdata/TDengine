using OSIsoft.AF.Asset;
using System.Collections.Generic;
using System.Linq;

namespace TDPIConnector.PI
{
    public class AFAttributeTemplatesWrapper : List<AFAttributeTemplateWrapper>
    {
        internal AFAttributeTemplatesWrapper(AFAttributeTemplates attributes)
        {
            foreach (AFAttributeTemplate attribute in attributes)
            {
                this.Add(new AFAttributeTemplateWrapper(attribute));
            }
        }

        internal void AppendAFAttributeTemplates(AFAttributeTemplates attributes)
        {
            HashSet<string> nameList = new HashSet<string>();
            foreach (var attribute in this)
            {
                nameList.Add(attribute.Name);
            }
            foreach (AFAttributeTemplate attribute in attributes)
            {
                if (nameList.Contains(attribute.Name)) continue;
                this.Add(new AFAttributeTemplateWrapper(attribute));
            }
        }

        public AFAttributeTemplatesWrapper()
        {

        }

        public AFAttributeTemplateWrapper this[string attributeName]
        {
            get
            {
                return this.Where(a => a.Name == attributeName).SingleOrDefault();
            }
        }
    }
}
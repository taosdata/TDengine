using System.Linq;
using System.Collections.Generic;
using TDEngineHttpClient.Models;
using PISystemWrapper;

namespace TDEngineConnectionTest
{
    internal class AttributeColumnConverter
    {
        internal static IEnumerable<TDColumn> Convert(AFAttributeTemplatesWrapper attributeTemplates)
        {
            List<TDColumn> list = new List<TDColumn>();
            foreach (AFAttributeTemplateWrapper attributeTemplate in attributeTemplates)
            {
                TDColumn column = ConvertAttribute(attributeTemplate);
                list.Add(column);
            }
            return list.OrderBy(item => item.Name);
        }

        private static TDColumn ConvertAttribute(AFAttributeTemplateWrapper attributeTemplate)
        {
            string tdColumnType = AttributeTypeConverter.Convert(attributeTemplate.Type);
            return new TDColumn(attributeTemplate.Name, tdColumnType, attributeTemplate.Uom);
        }
    }
}
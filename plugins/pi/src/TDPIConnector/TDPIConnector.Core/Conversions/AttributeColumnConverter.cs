using System.Linq;
using System.Collections.Generic;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.PI;

namespace TDPIConnector.Core.Conversions
{
    internal class AttributeColumnConverter
    {
        internal static IEnumerable<TDColumn> Convert(AFAttributeTemplatesWrapper attributeTemplates)
        {
            List<TDColumn> list = new List<TDColumn>();
            foreach (AFAttributeTemplateWrapper attributeTemplate in attributeTemplates)
            {
                TDColumn column = ConvertAttribute(attributeTemplate);
                if (null == column) continue;
                list.Add(column);
            }
            return list.OrderBy(item => item.Name);
        }

        internal static IEnumerable<TDColumn> Convert(AFAttributesWrapper attributes)
        {
            List<TDColumn> list = new List<TDColumn>();
            foreach (AFAttributeWrapper attribute in attributes)
            {
                TDColumn column = ConvertElementAttribute(attribute);
                if (null == column) continue;
                list.Add(column);
            }
            return list.OrderBy(item => item.Name);
        }

        private static TDColumn ConvertAttribute(AFAttributeTemplateWrapper attributeTemplate)
        {
            string tdColumnType = AttributeTypeConverter.Convert(attributeTemplate.DataReference, attributeTemplate.Type);
            if (null == tdColumnType) return null;
            return new TDColumn(attributeTemplate.Name, tdColumnType, attributeTemplate.Uom, attributeTemplate.DataReference);
        }
        private static TDColumn ConvertElementAttribute(AFAttributeWrapper attribute)
        {
            string tdColumnType = AttributeTypeConverter.Convert(attribute.DataReference, attribute.Type);
            if (null == tdColumnType) return null;
            return new TDColumn(attribute.Name, tdColumnType, attribute.Uom, attribute.DataReference);
        }
    }
}
using System;
using System.Collections.Generic;
using TDPIConnector.PI;

namespace TDPIConnector.Core.Conversions
{
    internal class ConfigurationItemConverter
    {
        internal static List<string> Convert(AFAttributesWrapper attributes)
        {
            List<string> items = new List<string>();
            foreach (AFAttributeWrapper attribute in attributes)
            {
                if (attribute.IsConfigurationItem)
                {
                    items.Add(attribute.GetValue().ToString());
                }
        
            }
            return items;
        }
    }
}
using PISystemWrapper;
using System;
using System.Collections.Generic;

namespace TDEngineConnectionTest
{
    internal class UomConverter
    {
        internal static List<string> Convert(AFAttributesWrapper attributes)
        {
            List<string> uoms = new List<string>();
            foreach (AFAttributeWrapper attribute in attributes)
            {
                uoms.Add(attribute.Uom);
            }
            return uoms;
        }
    }
}
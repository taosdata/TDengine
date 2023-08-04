using System.Collections.Generic;
using TDPIConnector.TDEngine.Models;
using TDPIConnector.PI;
using TDPIConnector.TDEngine.Helper;

namespace TDPIConnector.Core.Conversions
{
    internal class TemplateSTableConverter
    {
        internal static TDSTable Convert(AFElementTemplateWrapper template)
        {
            var sTable = new TDSTable(template.Name)
            {
                Columns = AttributeColumnConverter.Convert(template.AttributeTemplates)
            };
            return sTable;
        }
    }
    
    internal class ElemenetTableConverter
    {
        internal static TDTable Convert(AFElementWrapper element, string sTableName, IEnumerable<TDColumn> columns)
        {
            var location = getLocation(element.GetPath());
            Dictionary<string, string> tags = new Dictionary<string, string>();
            var elementColumns = new List<TDColumn>();
            foreach (var c in columns)
            {
                elementColumns.Add(new TDColumn(c));
            }

            foreach (var attr in element.Attributes)
            {
                if (string.IsNullOrEmpty(attr.DataReference))
                {
                    var value = attr.ToStringWithUOM();
                    tags.Add(attr.Name.ToTDEngineNamingPattern(), value);
                }
            }
            foreach (var column in elementColumns)
            {
                if (tags.ContainsKey(column.Name))
                {
                    column.TagValue = tags[column.Name];
                }
            }
            var table = new TDTable(element.Name, element.ID.ToString(), sTableName)
            {
                Columns = elementColumns,
                Location = location
            };
            return table;
        }
        static string getLocation(string path)
        {
            // "\\\\WIN-2OA23UM12TN\\Meters\\California\\San Francisco\\Meter_10001"
            string[] parts = path.Split('\\');
            int startIndex = 0;
            for (int i = 0; i < parts.Length; i++) {
                if (parts[i] == AppSettings.tomlConfig.AFDatabaseName) {
                    startIndex = i + 1;
                    break;
                }
            }
            if (parts.Length > startIndex)
            {
                string result = string.Join(".", parts, startIndex, parts.Length - startIndex - 1);
                return result;
            }

            return "";
        }
    }
}
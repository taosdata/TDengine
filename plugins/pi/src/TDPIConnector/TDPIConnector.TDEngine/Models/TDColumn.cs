using TDPIConnector.TDEngine.Helper;

namespace TDPIConnector.TDEngine.Models
{
    public class TDColumn
    {
        public TDColumn(string name, string tdColumnType, string uom, string configurationItem)
        {
            Name = name.ToTDEngineNamingPattern(); 
            Type = tdColumnType;
            Uom = uom;
            ConfigurationItem = configurationItem;
            IsTag = string.IsNullOrEmpty(uom) || string.IsNullOrEmpty(configurationItem);
        }

        public string Uom { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string ConfigurationItem { get; set; }
        public bool IsTag { get; set; }


    }
}

using TDPIConnector.TDEngine.Helper;

namespace TDPIConnector.TDEngine.Models
{
    public class TDColumn
    {
        public TDColumn(string name, string tdColumnType, string uom, string dataReference)
        {
            Name = name.ToTDEngineNamingPattern(); 
            Type = tdColumnType;
            Uom = uom;
            DataReference = dataReference;
        }
        public TDColumn(TDColumn r)
        {
            Name = r.Name;
            Type = r.TagValue;
            Uom = r.Uom;
            DataReference = r.DataReference;
        }
        public string Uom { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string DataReference { get; set; }
        public string TagValue { get; set; }
        public bool IsTag()
        {
            return string.IsNullOrEmpty(DataReference);
        }
    }
}

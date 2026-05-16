namespace TDEngineDR.TDEngineClient.Models
{
    public class TDColumn
    {
        public TDColumn(string name, string tdColumnType, string uom)
        {
            Name = name.ToLower().Replace(" ", "_");
            Type = tdColumnType;
            Uom = uom;
        }

        public string Uom { get; set; }

        public string Name { get; set; }

        public string Type { get; set; }
    }
}

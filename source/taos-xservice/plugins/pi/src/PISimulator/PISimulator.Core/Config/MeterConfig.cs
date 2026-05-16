namespace PISimulator.Core.Config
{
    public class MeterConfig
    {
        public int Id { get; set; }
        public DataTypeEnum Type { get; set; }
        public int TimePeriod { get; set; }
        public MeterAttributeConfig Current { get; set; }

        public MeterAttributeConfig Voltage { get; set; }
    }



}

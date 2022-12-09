namespace Consumer
{
    //const string createTable = "CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);";

    public class MeterTag
    {
        public string Location { get; set; }
        public int GroupID { get; set; }

        public MeterTag(string location, int groupId)
        {
            Location = location;
            GroupID = groupId;
        }
        public override string ToString()
        {
            return $"location:{Location},GroupID:{GroupID}";
        }
    }

    public class MeterValues
    {
        public long TimeStamp { get; set; }
        public float Current { get; set; }

        public int Voltage { get; set; }
        public float Phase { get; set; }

        public MeterValues(long ts, float current, int voltage, float phase)
        {
            TimeStamp = ts;
            Current = current;
            Voltage = voltage;
            Phase = phase;
        }

        public override string ToString()
        {
            return $"{TimeStamp},{Current},{Voltage},{Phase}";
        }
    }
}

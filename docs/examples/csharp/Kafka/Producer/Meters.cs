using Newtonsoft.Json;
namespace Producer
{
    //const string createTable = "CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);";

    public class MeterTag
    {
        public string Location { get; set; }
        public int GroupID { get; set; }

        public MeterTag(string location, int gourpId)
        {
            Location = location;
            GroupID = gourpId;
        }
        public override string ToString()
        {
            return $"location:{Location},GroupID:{GroupID}";
        }
    }

    public class MeterValues
    {
        public long Timestamp { get; set; }
        public float Current { get; set; }


        public int Voltage { get; set; }
        public float Phase { get; set; }

        public MeterValues(long ts, float current, int voltage, float phase)
        {
            Timestamp = ts;
            Current = current;
            Voltage = voltage;
            Phase = phase;
        }

        public override string ToString()
        {
            return $"{Timestamp},{Current},{Voltage},{Phase}";
        }
    }
}

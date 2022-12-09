using Confluent.Kafka;

namespace Producer
{
    
    public class MessageGenerator
    {
        string[] LOCATION = { "Los Angeles,CA", "San Diego,CA", " San Jose,CA", "San Francisco,CA", "Fresno,CA", "Sacramento,CA", "Oakland,CA", "Bakersfield,CA", "Anaheim,CA", "Long Beach,CA" };
        int[] GROUP_ID = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        internal long ts = 1667232000000;
        Random random = new Random();
        
        public MeterTag GenKey()
        {
            lock (this) {
                return new MeterTag(LOCATION[random.Next(0, 9)], GROUP_ID[random.Next(0, 9)]);
            }
               
        }

        public MeterValues GenValue()
        {
            lock (this) 
            {
                this.ts += 1;
                float current = random.NextSingle() + 10;
                int voltage = random.Next(210, 229);
                float phase = random.NextSingle();
                return new MeterValues(ts, current, voltage, phase);
            }
            
            
            
        }

    }
}

namespace TDPIConnector.Core.Monitoring.Models
{
    public class EventsPerAttribute
    {
        public EventsPerAttribute(string attributePath, int eventsNumber)
        {
            this.AttributePath = attributePath;
            this.Events = eventsNumber;
        }

        public string AttributePath { get; set; }

        public int Events { get; set; }
    }
}

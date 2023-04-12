namespace TDPIConnector.Core.Monitoring.Models
{
    public class EventsPerPoint
    {
        public EventsPerPoint(string pointName, int eventsNumber)
        {
            this.PointName = pointName;
            this.Events = eventsNumber;
        }

        public string PointName { get; set; }

        public int Events { get; set; }
    }
}

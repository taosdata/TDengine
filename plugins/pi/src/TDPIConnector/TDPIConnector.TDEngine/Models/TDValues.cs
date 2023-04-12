using System.Collections.Generic;

namespace TDPIConnector.TDEngine.Models
{
    public class TDValues : List<TDValue>
    {
        public TDValues(IList<TDValue> values)
        {
            this.AddRange(values);
        }
    }
}

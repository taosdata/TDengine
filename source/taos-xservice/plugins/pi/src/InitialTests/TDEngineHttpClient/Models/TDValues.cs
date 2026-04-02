using System;
using System.Collections.Generic;

namespace TDEngineHttpClient.Models
{
    public class TDValues : List<TDValue>
    {
        public TDValues(IList<TDValue> values)
        {
            this.AddRange(values);
        }
    }
}

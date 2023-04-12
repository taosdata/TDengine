using System;
using System.Collections.Generic;

namespace TDEngineDR.TDEngineClient.Models
{
    public class TDValues : List<TDValue>
    {
        public TDValues()
        {

        }
        public TDValues(IList<TDValue> values)
        {
            this.AddRange(values);
        }
    }
}

using System;

namespace TDEngineDR.TDEngineClient.Exceptions
{
    class TDTableNotFoundException : Exception
    {
        public TDTableNotFoundException() : base("Table not found on TDengine")
        {
                
        }
    }
}

using System;

namespace TDEngineDR.TDEngineClient.Models
{
    [Flags]
    public enum TDSummaryTypes
    {
        None = 0,      
        Total = 1,     
        Average = 2,     
        Minimum = 4,     
        Maximum = 8,    
        Range = 16,      
        StdDev = 32,    
        PopulationStdDev = 64,     
        Count = 128,    
        PercentGood = 8192,    
        All = 24831
    }
}

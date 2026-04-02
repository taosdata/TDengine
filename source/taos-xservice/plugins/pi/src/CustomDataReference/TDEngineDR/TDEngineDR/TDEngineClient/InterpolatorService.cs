using System;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR.TDEngineClient
{
    internal class InterpolatorService
    {
        internal static TDValue InterpolateTwoValues(DateTime timestamp, TDValues valuesBackward, TDValues valuesForward)
        {
            TDValue valueBackward;
            TDValue valueForward;
            if (valuesBackward == null || valuesBackward.Count == 0)
            {
                
                valueForward = valuesForward[0];
                valueForward.Timestamp = timestamp;
                return valueForward;
            }

            if (valuesForward == null || valuesForward.Count == 0)
            {
                valueBackward = valuesBackward[0];
                valueBackward.Timestamp = timestamp;
                return valueBackward;
            }
            valueBackward = valuesBackward[0];
            valueForward = valuesForward[0];
            if (valueForward.Timestamp == valueBackward.Timestamp)
            {
                return valueBackward;
            }
            double k = (timestamp - valueBackward.Timestamp).TotalSeconds * 1.0 / (valueForward.Timestamp - valueBackward.Timestamp).TotalSeconds;
            double value = valueBackward.GetValueAsDouble() + k * (valueForward.GetValueAsDouble() - valueBackward.GetValueAsDouble());
            return new TDValue(value, timestamp, 0, TDValueType.Double);
        }
    }
}
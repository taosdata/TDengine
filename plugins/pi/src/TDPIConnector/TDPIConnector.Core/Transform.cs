using System.Collections.Generic;
using System.Linq;
using System;
using Newtonsoft.Json;
using TDPIConnector.PI;
using TDPIConnector.Core.Conversions;

namespace TDPIConnector.Core
{
    class Transform
    {
        static public string GeneratePointSuperTableName(PIPointWrapper point)
        {
            return "ts_" + point.PointType.ToLower();
        }
    }
}

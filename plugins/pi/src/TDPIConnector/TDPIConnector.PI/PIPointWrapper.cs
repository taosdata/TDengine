using OSIsoft.AF.PI;
using System.Collections.Generic;

namespace TDPIConnector.PI
{
    public class PIPointWrapper: AFObjectWrapper<PIPoint>
    {


        public PIPointWrapper(PIPoint piPoint): base(piPoint)
        {
            this.PointType = piPoint.PointType.ToString();
            this.PointId = piPoint.ID;
        }

        public PIPointWrapper(PIPoint piPoint, string pointType, int pointId) : this(piPoint)
        {
            this.PointType = pointType;
            this.PointId = pointId;
        }

        public int ID
        {
            get
            {
                return this.AFSDKObject.ID;
            }
        }

        public string Name
        {
            get
            {
                return this.AFSDKObject.Name;
            }
        }

        public string PointType { get; private set; }
        public int PointId { get; private set; }

        public object GetAttribute(string name)
        {
            return this.AFSDKObject.GetAttribute(name);
        }

        public void SaveAttributes(Dictionary<string, object> piPointAttributes)
        {
            this.AFSDKObject.SaveAttributes(piPointAttributes);
        }


    }
}

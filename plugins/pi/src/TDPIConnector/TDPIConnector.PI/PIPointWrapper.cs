using OSIsoft.AF.PI;
using System.Collections.Generic;
using System.Linq;

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
        static public Dictionary<string, string> GetPointSavedAttrsType()
        {
            Dictionary<string, string> tags = new Dictionary<string, string> { };
            tags.Add("ptclassname", "string");
            tags.Add("sourcetag", "string");
            tags.Add("tag", "string");
            tags.Add("descriptor", "string");
            tags.Add("exdesc", "string");
            tags.Add("engunits", "string");
            tags.Add("pointsource", "string");
            tags.Add("step", "string");
            tags.Add("future", "string");
            return tags;
        }
        public Dictionary<string, string> GetPointSavedAttrsValue() {
            // var all = this.AFSDKObject.FindAttributeNames("*");
            string[] needSaveAttr = {"ptclassname", "sourcetag", "tag", "descriptor", "exdesc", "engunits", "pointsource", "step", "future"};
            IDictionary<string, object> res = this.AFSDKObject.GetAttributes(needSaveAttr.ToArray());
            Dictionary<string, string> tags = new Dictionary<string, string> { };
            foreach (var r in res) {
                var k = r.Key;
                var v = r.Value.GetType();
                tags.Add(r.Key, r.Value.ToString());
            }
            return tags;
        }

        public string GetPath() {
            return this.AFSDKObject.GetPath();
        }
    }
}

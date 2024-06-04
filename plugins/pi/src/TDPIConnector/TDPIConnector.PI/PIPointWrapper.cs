using OSIsoft.AF.PI;
using System.Collections.Generic;
using System.Linq;
using TDPIConnector.TDEngine.TaosxClient;

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
            Dictionary<string, string> tags = new Dictionary<string, string>
            {
                { "ptclassname", IpcDataTypes.VarCharType },
                { "sourcetag", IpcDataTypes.VarCharType },
                { "tag", IpcDataTypes.VarCharType },
                { "descriptor", IpcDataTypes.VarCharType },
                { "exdesc", IpcDataTypes.VarCharType },
                { "engunits", IpcDataTypes.VarCharType },
                { "pointsource", IpcDataTypes.VarCharType },
                { "step", IpcDataTypes.VarCharType },
                { "future", IpcDataTypes.VarCharType }
            };
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

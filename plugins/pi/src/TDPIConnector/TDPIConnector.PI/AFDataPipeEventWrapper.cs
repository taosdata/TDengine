using OSIsoft.AF.Data;

namespace TDPIConnector.PI
{
    public class AFDataPipeEventWrapper : AFObjectWrapper<AFDataPipeEvent>
    {
        internal AFDataPipeEventWrapper(AFDataPipeEvent dataPipeEvent) : base(dataPipeEvent)
        {
        }
        public AFDataPipeAction AFEventAction()
        {
            return this.AFSDKObject.Action;
        }
        public virtual AFValueWrapper Value
        {
            get
            {
                return new AFValueWrapper(this.AFSDKObject.Value);
            }
        }
    }
}

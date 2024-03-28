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
        public AFDataPipePreviousEventAction PreviousEventAction
        {
            get
            {
                return this.AFSDKObject.PreviousEventAction;
            }
        }
        public AFValueWrapper SpecificUpdatedValue
        {
            get
            {
                return new AFValueWrapper(this.AFSDKObject.SpecificUpdatedValue);
            }
        }
        public bool IsAFDataPipeRangeDeletedEvent()
        {
            if (this.AFSDKObject is AFDataPipeRangeDeletedEvent)
                return true;
            return false;
        }
        public AFDataPipeRangeDeletedEventWrapper ToAFDataPipeRangeDeletedEventWrapper() {
            return new AFDataPipeRangeDeletedEventWrapper((AFDataPipeRangeDeletedEvent)AFSDKObject);
        }
    }
    public class AFDataPipeRangeDeletedEventWrapper : AFObjectWrapper<AFDataPipeRangeDeletedEvent> {
        internal AFDataPipeRangeDeletedEventWrapper(AFDataPipeRangeDeletedEvent dataPipeEvent) : base(dataPipeEvent)
        {

        }
        public AFTimeWrapper StartTime {
            get { return new AFTimeWrapper(this.AFSDKObject.StartTime); }
        }
        public AFTimeWrapper EndTime
        {
            get { return new AFTimeWrapper(this.AFSDKObject.EndTime); }
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
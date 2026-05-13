using OSIsoft.AF;
using System;

namespace TDPIConnector.PI
{
    public class AFDatabaseWrapper: AFObjectWrapper<AFDatabase>
    {
        
        public AFDatabaseWrapper(AFDatabase database) : base(database)
        {
        }

        public Guid ID
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


        public void CheckIn()
        {
            this.AFSDKObject.CheckIn(AFCheckedOutMode.ObjectsCheckedOutThisSession);
        }
    }
}
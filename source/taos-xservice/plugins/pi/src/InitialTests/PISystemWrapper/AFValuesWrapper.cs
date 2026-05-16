using OSIsoft.AF.Asset;

namespace PISystemWrapper
{
    public class AFValuesWrapper
    {
        private AFValues AFSDKObject;
        internal AFValuesWrapper(AFValues values)
        {
            this.AFSDKObject = values;
        }

        public int Count
        {
            get
            {
                return this.AFSDKObject.Count;
            }
        }

        public AFValueWrapper this[int index]
        {
            get
            {
                return new AFValueWrapper(this.AFSDKObject[index]);
            }
        }

    }
}

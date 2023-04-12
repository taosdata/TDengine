namespace PISystemWrapper
{
    public abstract class AFObjectWrapper<T>
    {
        internal AFObjectWrapper(T afSdkObject)
        {
            this.AFSDKObject = afSdkObject;
        }
        internal T AFSDKObject { get; set; }
    }
}

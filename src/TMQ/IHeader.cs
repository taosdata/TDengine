namespace TDengine.TMQ
{
    public interface IHeader
    {
        string Key { get; }
        byte[] GetValueBytes();
    }
}
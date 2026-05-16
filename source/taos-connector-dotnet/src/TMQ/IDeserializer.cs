namespace  TDengine.TMQ
{
    public interface IDeserializer<T>
    {
        T Deserialize(ITMQRows data, bool isNull, SerializationContext context);
    }
}

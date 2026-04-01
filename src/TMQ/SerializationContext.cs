namespace TDengine.TMQ
{
    public class SerializationContext
    {
        public static SerializationContext Empty
            => default(SerializationContext);
        public SerializationContext(MessageComponentType component, string topic, Headers headers = null)
        {
            Component = component;
            Topic = topic;
            Headers = headers;
        }
        public string Topic { get; private set; }
        public MessageComponentType Component { get; private set; }
        
        public Headers Headers { get; private set; }
    }
}
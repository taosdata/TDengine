using System;
using System.Collections.Generic;
using TDengine.Driver;

namespace TDengine.TMQ
{
    public class ConsumerBuilder<TValue>
    {
        protected internal IEnumerable<KeyValuePair<string, string>> Config { get; set; }
        
        protected internal IDeserializer<TValue> ValueDeserializer { get; set; }
        

        
        public ConsumerBuilder<TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)
        {
            if (this.ValueDeserializer != null)
            {
                throw new InvalidOperationException("Value deserializer may not be specified more than once.");
            }
            this.ValueDeserializer = deserializer;
            return this;
        }
        
        public IConsumer<TValue> Build()
        {
            var connectType = TDengineConstant.ProtocolNative;
            foreach (var kv in Config)
            {
                if (kv.Key == "td.connect.type")
                {
                    connectType = kv.Value;
                }
            }

            switch (connectType)
            {
                case TDengineConstant.ProtocolWebSocket:
                    return new WebSocket.Consumer<TValue>(this);
                case TDengineConstant.ProtocolNative:
                    return new Native.Consumer<TValue>(this);
                default:
                    throw new ArgumentException($"Unsupported connect type: {connectType}");
            }
        }

        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            Config = config;
        }
    }
}
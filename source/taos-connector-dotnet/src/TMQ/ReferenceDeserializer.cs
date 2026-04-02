using System;
using System.Collections.Generic;
using TDengine.Driver;

namespace TDengine.TMQ
{
    public class ReferenceDeserializer<T> : IDeserializer<T> where T : class
    {
        public T Deserialize(ITMQRows result, bool isNull, SerializationContext context)
        {
            if (isNull) return null;

            var obj = Activator.CreateInstance<T>();
            for (int col = 0; col < result.FieldCount; col++)
            {
                var name = result.GetName(col);
                var type = typeof(T);
                var property = type.GetProperty(name);
                if (property != null && property.CanWrite)
                {
                    var value = result.GetValue(col);
                    try
                    {
                        property.SetValue(obj, value);
                    }
                    catch (Exception e)
                    {
                        var exception = new Exception(
                            $"Failed to set property {name} of type {property.PropertyType} with value {value} value type {value.GetType()}",
                            e);
                        throw exception;
                    }
                }
            }

            return obj;
        }
    }
}
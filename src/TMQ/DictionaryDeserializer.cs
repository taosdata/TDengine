using System.Collections.Generic;
using TDengine.Driver;
using TDengine.TMQ;

namespace TDengineHelper
{
    public class DictionaryDeserializer : IDeserializer<Dictionary<string, object>>
    {
        public static IDeserializer<Dictionary<string, object>> Dictionary = new DictionaryDeserializer();
        public Dictionary<string, object> Deserialize(ITMQRows result, bool isNull, SerializationContext context)
        {
            if (isNull) return null;

            var obj = new Dictionary<string, object>();
            for (int col = 0; col < result.FieldCount; col++)
            {
                var name = result.GetName(col);
                obj[name] =  result.GetValue(col);
            }
            return obj;
        }
    }
}
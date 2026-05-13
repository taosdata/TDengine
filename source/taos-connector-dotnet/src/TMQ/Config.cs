using System;
using System.Collections;
using System.Collections.Generic;

namespace TDengine.TMQ
{
    /// <summary>
    /// Base class for TDengine TMQ config
    /// </summary>
    public class Config : IEnumerable<KeyValuePair<string, string>>
    {

        protected IDictionary<string, string> properties;

        public Config()
        {
            this.properties = new Dictionary<string, string>();
        }

        public Config(IDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public Config(Config config)
        {
            this.properties = config.properties;
        }


        /// <summary>
        /// Set a configuration property using a string key/value pair.
        /// </summary>
        /// <param name="key"> The configuration property name.</param>
        /// <param name="value"> The property value. </param>
        public void Set(string key, string value)
        {
            this.properties[key] = value;
        }

        /// <summary>
        /// Gets a configuration property value given a key. Return null 
        /// if the property has not been set.
        /// </summary>
        /// <param name="key"> The configuration property to get.</param>
        /// <returns> The configuration property value.</returns>
        public string Get(string key)
        {
            if (this.properties.TryGetValue(key, out var value))
            {
                return value;
            }
            return null;
        }

        /// <summary>
        /// Gets a configuration property value given a key. Return null 
        /// if the property has not been set.
        /// </summary>
        /// <param name="key"> The configuration property to get.</param>
        /// <returns> The configuration property value.</returns>
        public int? GetInt(string key)
        {
            var result = this.properties[key];
            if (result == null)
            {
                return null;
            }
            return int.Parse(result);

        }

        /// <summary>
        /// Gets a configuration property value given a key. Return null 
        /// if the property has not been set.
        /// </summary>
        /// <param name="key"> The configuration property to get.</param>
        /// <returns> The configuration property value.</returns>
        public bool? GetBool(string key)
        {
            var result = this.properties[key];
            if (result == null)
            {
                return null;
            }
            return bool.Parse(result);
        }


        /// <summary>
        /// Set a configuration property using a key/value pair(null checked)
        /// </summary>
        /// <param name="name"></param>
        /// <param name="val"></param>
        public void SetObject(string name, object val)
        {
            if (val == null)
            {
                this.properties.Remove(name);
                return;
            }

            if (val is System.Enum)
            {
                var stringVal = val.ToString();
                this.properties[name] = stringVal;
            }
            else
            {
                this.properties[name] = val.ToString();
            }
        }

        /// <summary>
        /// Gets a configuration property enum value given a key.
        /// </summary>
        /// <param name="type"> The configuration property to get.</param>
        /// <param name="key">The enum type of the configuration property.</param>
        /// <returns> The configuration property value.</returns>
        public object GetEnum(Type type, string key)
        {
            var result = Get(key);
            if (result == null)
            {
                return null;
            }
            return Enum.Parse(type, result, ignoreCase: true);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return this.properties.GetEnumerator();
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.properties.GetEnumerator();
        }
    }
}

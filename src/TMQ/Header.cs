using System;

namespace TDengine.TMQ
{
    public class Header : IHeader
    {
        private byte[] val;
    
        public string Key { get; private set; }
    
        public byte[] GetValueBytes()
        {
            return val;
        }

        public Header(string key, byte[] value)
        {
            if (key == null)
            {
                throw new ArgumentNullException("tmq message header key cannot be null.");
            }

            Key = key;
            val = value;
        }
    }
}
using System.Collections;
using System.Collections.Generic;

namespace TDengine.TMQ
{
    public class Headers : IEnumerable<IHeader>
    {
        private readonly List<IHeader> headers = new List<IHeader>();
        
        public IReadOnlyList<IHeader> BackingList => headers;
        
        public void Add(string key, byte[] val)
        {
            if (key == null)
            {
                throw new System.ArgumentNullException("tmq message header key cannot be null.");
            }

            headers.Add(new Header(key, val));
        }
        
        internal class HeadersEnumerator : IEnumerator<IHeader>
        {
            private Headers headers;

            private int location = -1;

            public HeadersEnumerator(Headers headers)
            {
                this.headers = headers;
            }

            public object Current 
                => ((IEnumerator<IHeader>)this).Current;

            IHeader IEnumerator<IHeader>.Current
                => headers.headers[location];

            public void Dispose() {}

            public bool MoveNext()
            {
                location += 1;
                if (location >= headers.headers.Count)
                {
                    return false;
                }

                return true;
            }

            public void Reset()
            {
                this.location = -1;
            }
        }

        public IEnumerator<IHeader> GetEnumerator()
            => new HeadersEnumerator(this);

        IEnumerator IEnumerable.GetEnumerator()
            => new HeadersEnumerator(this);
        
        public IHeader this[int index]
            => headers[index];
        
        public int Count
            => headers.Count;
    }
}
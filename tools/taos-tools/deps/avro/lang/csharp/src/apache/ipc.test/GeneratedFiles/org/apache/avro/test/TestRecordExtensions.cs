namespace org.apache.avro.test
{
    public partial class TestRecord
    {
        protected bool Equals(TestRecord other)
        {
            return string.Equals(_name, other._name) && _kind == other._kind && Equals(_hash, other._hash);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((TestRecord) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (_name != null ? _name.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (int) _kind;
                hashCode = (hashCode*397) ^ (_hash != null ? _hash.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
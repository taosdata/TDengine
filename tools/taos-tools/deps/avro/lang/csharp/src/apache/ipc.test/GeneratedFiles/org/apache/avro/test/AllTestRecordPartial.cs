using System.Linq;

namespace org.apache.avro.test
{
    public partial class AllTestRecord
    {
        protected bool Equals(AllTestRecord other)
        {
            bool arrayEqual = _arrayTest.SequenceEqual(other._arrayTest);
            bool mapEqual = _mapTest.SequenceEqual(other._mapTest);
            bool bytesEqual = _bytesTest.SequenceEqual(other._bytesTest);

            return Equals(_nestedTest, other._nestedTest) && mapEqual &&
                   arrayEqual
                 && Equals(_fixedTest, other._fixedTest) &&
                   _enumTest == other._enumTest && string.Equals(_stringTest, other._stringTest) &&
                   bytesEqual && _doubleTest.Equals(other._doubleTest) &&
                   _floatTest.Equals(other._floatTest) && _longTest == other._longTest && _intTest == other._intTest &&
                   _booleanTest.Equals(other._booleanTest);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((AllTestRecord) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (_nestedTest != null ? _nestedTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (_mapTest != null ? _mapTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (_arrayTest != null ? _arrayTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (_fixedTest != null ? _fixedTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (int) _enumTest;
                hashCode = (hashCode*397) ^ (_stringTest != null ? _stringTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (_bytesTest != null ? _bytesTest.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ _doubleTest.GetHashCode();
                hashCode = (hashCode*397) ^ _floatTest.GetHashCode();
                hashCode = (hashCode*397) ^ _longTest.GetHashCode();
                hashCode = (hashCode*397) ^ _intTest;
                hashCode = (hashCode*397) ^ _booleanTest.GetHashCode();
                return hashCode;
            }
        }
    }
}